/* Asynchronous replication implementation.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "redis.h"
#include "hiredis.h"
#include "async.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ifaddrs.h>

/* Prototyping */
void syncWithSentinelConnected(const redisAsyncContext *c, int status);

/* ---------------------------------- MASTER -------------------------------- */

void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;

    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master. */
        if (slave->slaveseldb != dictid) {
            robj *selectcmd;

            if (dictid >= 0 && dictid < REDIS_SHARED_SELECT_CMDS) {
                selectcmd = shared.select[dictid];
                incrRefCount(selectcmd);
            } else {
                selectcmd = createObject(REDIS_STRING,
                    sdscatprintf(sdsempty(),"select %d\r\n",dictid));
            }
            addReply(slave,selectcmd);
            decrRefCount(selectcmd);
            slave->slaveseldb = dictid;
        }
        addReplyMultiBulkLen(slave,argc);
        for (j = 0; j < argc; j++) addReplyBulk(slave,argv[j]);
    }
}

void replicationFeedMonitors(redisClient *c, list *monitors, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, port;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    char ip[32];
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & REDIS_LUA_CLIENT) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & REDIS_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        anetPeerToString(c->fd,ip,&port);
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s:%d] ",dictid,ip,port);
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == REDIS_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(REDIS_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        redisClient *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

void syncCommand(redisClient *c) {
    /* ignore SYNC if already slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    if (server.masterhost && server.repl_state != REDIS_REPL_CONNECTED) {
        addReplyError(c,"Can't SYNC while not connected with my master");
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    if (listLength(c->reply) != 0) {
        addReplyError(c,"SYNC is invalid with pending input");
        return;
    }

    redisLog(REDIS_NOTICE,"Slave ask for synchronization");
    /* Here we need to check if there is a background saving operation
     * in progress, or if it is required to start one */
    if (server.rdb_child_pid != -1) {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save */
        redisClient *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) break;
        }
        if (ln) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer. */
            copyClientOutputBuffer(c,slave);
            c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
            redisLog(REDIS_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences */
            c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
            redisLog(REDIS_NOTICE,"Waiting for next BGSAVE for SYNC");
        }
    } else {
        /* Ok we don't have a BGSAVE in progress, let's start one */
        redisLog(REDIS_NOTICE,"Starting BGSAVE for SYNC");
        if (rdbSaveBackground(server.rdb_filename) != REDIS_OK) {
            redisLog(REDIS_NOTICE,"Replication failed, can't BGSAVE");
            addReplyError(c,"Unable to perform background save");
            return;
        }
        c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
    }

    if (server.repl_disable_tcp_nodelay)
        anetDisableTcpNoDelay(NULL, c->fd); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= REDIS_SLAVE;
    c->slaveseldb = 0;
    listAddNodeTail(server.slaves,c);
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a slave in order to configure the replication
 * process before starting it with the SYNC command.
 *
 * Currently the only use of this command is to communicate to the master
 * what is the listening port of the Slave redis instance, so that the
 * master can accurately list slaves and their listening ports in
 * the INFO output.
 *
 * In the future the same command can be used in order to configure
 * the replication to initiate an incremental replication instead of a
 * full resync. */
void replconfCommand(redisClient *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != REDIS_OK))
                return;
            c->slave_listening_port = port;
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

void sendBulkToSlave(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *slave = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    char buf[REDIS_IOBUF_LEN];
    ssize_t nwritten, buflen;

    if (slave->repldboff == 0) {
        /* Write the bulk write count before to transfer the DB. In theory here
         * we don't know how much room there is in the output buffer of the
         * socket, but in practice SO_SNDLOWAT (the minimum count for output
         * operations) will never be smaller than the few bytes we need. */
        sds bulkcount;

        bulkcount = sdscatprintf(sdsempty(),"$%lld\r\n",(unsigned long long)
            slave->repldbsize);
        if (write(fd,bulkcount,sdslen(bulkcount)) != (signed)sdslen(bulkcount))
        {
            sdsfree(bulkcount);
            freeClient(slave);
            return;
        }
        sdsfree(bulkcount);
    }
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    buflen = read(slave->repldbfd,buf,REDIS_IOBUF_LEN);
    if (buflen <= 0) {
        redisLog(REDIS_WARNING,"Read error sending DB to slave: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if ((nwritten = write(fd,buf,buflen)) == -1) {
        redisLog(REDIS_VERBOSE,"Write error sending DB to slave: %s",
            strerror(errno));
        freeClient(slave);
        return;
    }
    slave->repldboff += nwritten;
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
        slave->replstate = REDIS_REPL_ONLINE;
        if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE,
            sendReplyToClient, slave) == AE_ERR) {
            freeClient(slave);
            return;
        }
        redisLog(REDIS_NOTICE,"Synchronization with slave succeeded");
    }
}

/* This function is called at the end of every background saving.
 * The argument bgsaveerr is REDIS_OK if the background saving succeeded
 * otherwise REDIS_ERR is passed to the function.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization. */
void updateSlavesWaitingBgsave(int bgsaveerr) {
    listNode *ln;
    int startbgsave = 0;
    listIter li;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) {
            startbgsave = 1;
            slave->replstate = REDIS_REPL_WAIT_BGSAVE_END;
        } else if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != REDIS_OK) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }
            if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                redis_fstat(slave->repldbfd,&buf) == -1) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                continue;
            }
            slave->repldboff = 0;
            slave->repldbsize = buf.st_size;
            slave->replstate = REDIS_REPL_SEND_BULK;
            aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);
            if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendBulkToSlave, slave) == AE_ERR) {
                freeClient(slave);
                continue;
            }
        }
    }
    if (startbgsave) {
        if (rdbSaveBackground(server.rdb_filename) != REDIS_OK) {
            listIter li;

            listRewind(server.slaves,&li);
            redisLog(REDIS_WARNING,"SYNC failed. BGSAVE failed");
            while((ln = listNext(&li))) {
                redisClient *slave = ln->value;

                if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START)
                    freeClient(slave);
            }
        }
    }
}

/* ----------------------------------- SLAVE -------------------------------- */


/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

typedef struct redisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleRead(e->context);
}

static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleWrite(e->context);
}

static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return REDIS_ERR;

    /* Create container for context and r/w events */
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return REDIS_OK;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master */
void replicationAbortSyncTransfer(void) {
    redisAssert(server.repl_state == REDIS_REPL_TRANSFER);

    aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);
    close(server.repl_transfer_s);
    close(server.repl_transfer_fd);
    unlink(server.repl_transfer_tmpfile);
    zfree(server.repl_transfer_tmpfile);

    server.repl_state = REDIS_REPL_CONNECT;
}

/* Asynchronously read the SYNC payload we receive from a master */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[4096];
    ssize_t nread, readlen;
    off_t left;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    if (server.repl_transfer_size == -1) {
        if (syncReadLine(fd,buf,1024,server.repl_syncio_timeout*1000) == -1) {
            redisLog(REDIS_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        }

        if (buf[0] == '-') {
            redisLog(REDIS_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            redisLog(REDIS_WARNING,"Bad protocol from MASTER, the first byte is not '$', are you sure the host and port are right?");
            goto error;
        }
        server.repl_transfer_size = strtol(buf+1,NULL,10);
        redisLog(REDIS_NOTICE,
            "MASTER <-> SLAVE sync: receiving %ld bytes from master",
            server.repl_transfer_size);
        return;
    }

    /* Read bulk data */
    left = server.repl_transfer_size - server.repl_transfer_read;
    readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
    nread = read(fd,buf,readlen);
    if (nread <= 0) {
        redisLog(REDIS_WARNING,"I/O error trying to sync with MASTER: %s",
            (nread == -1) ? strerror(errno) : "connection lost");
        redisLog(REDIS_WARNING, "Received: %llu bytes. Total: %llu bytes",
                 server.repl_transfer_read, server.repl_transfer_size);
        replicationAbortSyncTransfer();
        return;
    }
    server.repl_transfer_lastio = server.unixtime;
    if (write(server.repl_transfer_fd,buf,nread) != nread) {
        redisLog(REDIS_WARNING,"Write error or short write writing to the DB dump file needed for MASTER <-> SLAVE synchronization: %s", strerror(errno));
        goto error;
    }
    server.repl_transfer_read += nread;

    /* Sync data on disk from time to time, otherwise at the end of the transfer
     * we may suffer a big delay as the memory buffers are copied into the
     * actual disk. */
    if (server.repl_transfer_read >=
        server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
    {
        off_t sync_size = server.repl_transfer_read -
                          server.repl_transfer_last_fsync_off;
        rdb_fsync_range(server.repl_transfer_fd,
            server.repl_transfer_last_fsync_off, sync_size);
        server.repl_transfer_last_fsync_off += sync_size;
    }

    /* Check if the transfer is now complete */
    if (server.repl_transfer_read == server.repl_transfer_size) {
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            redisLog(REDIS_WARNING,"Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s", strerror(errno));
            replicationAbortSyncTransfer();
            return;
        }
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Loading DB in memory");
        signalFlushedDb(-1);
        emptyDb();
        /* Before loading the DB into memory we need to delete the readable
         * handler, otherwise it will get called recursively since
         * rdbLoad() will call the event loop to process events from time to
         * time for non blocking loading. */
        aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);
        if (rdbLoad(server.rdb_filename) != REDIS_OK) {
            redisLog(REDIS_WARNING,"Failed trying to load the MASTER synchronization DB from disk");
            replicationAbortSyncTransfer();
            return;
        }
        /* Final setup of the connected slave <- master link */
        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        server.master = createClient(server.repl_transfer_s);
        server.master->flags |= REDIS_MASTER;
        server.master->authenticated = 1;
        server.repl_state = REDIS_REPL_CONNECTED;
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Finished with success");
        /* Restart the AOF subsystem now that we finished the sync. This
         * will trigger an AOF rewrite, and when done will start appending
         * to the new file. */
        if (server.aof_state != REDIS_AOF_OFF) {
            int retry = 10;

            stopAppendOnly();
            while (retry-- && startAppendOnly() == REDIS_ERR) {
                redisLog(REDIS_WARNING,"Failed enabling the AOF after successful master synchronization! Trying it again in one second.");
                sleep(1);
            }
            if (!retry) {
                redisLog(REDIS_WARNING,"FATAL: this slave instance finished the synchronization with its master, but the AOF can't be turned on. Exiting now.");
                exit(1);
            }
        }
    }

    return;

error:
    replicationAbortSyncTransfer();
    return;
}

/* Send a synchronous command to the master. Used to send AUTH and
 * REPLCONF commands before starting the replication with SYNC.
 *
 * On success NULL is returned.
 * On error an sds string describing the error is returned.
 */
char *sendSynchronousCommand(int fd, ...) {
    va_list ap;
    sds cmd = sdsempty();
    char *arg, buf[256];

    /* Create the command to send to the master, we use simple inline
     * protocol for simplicity as currently we only send simple strings. */
    va_start(ap,fd);
    while(1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;

        if (sdslen(cmd) != 0) cmd = sdscatlen(cmd," ",1);
        cmd = sdscat(cmd,arg);
    }
    cmd = sdscatlen(cmd,"\r\n",2);

    /* Transfer command to the server. */
    if (syncWrite(fd,cmd,sdslen(cmd),server.repl_syncio_timeout*1000) == -1) {
        sdsfree(cmd);
        return sdscatprintf(sdsempty(),"Writing to master: %s",
                strerror(errno));
    }
    sdsfree(cmd);

    /* Read the reply from the server. */
    if (syncReadLine(fd,buf,sizeof(buf),server.repl_syncio_timeout*1000) == -1)
    {
        return sdscatprintf(sdsempty(),"Reading from master: %s",
                strerror(errno));
    }

    /* Check for errors from the server. */
    if (buf[0] != '+') {
        return sdscatprintf(sdsempty(),"Error from master: %s", buf);
    }

    return NULL; /* No errors. */
}

/* Callback for 'group join' from sentinel.
 * Response might contain a PENDING status, TRYAGAIN status or a response telling us we are master or slaves.
*/
void syncWithSentinelJoinGroupCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = NULL;
    sentinelInfo *sentinel = (sentinelInfo*)c->data;
    if (server.sentinel_conn_state == REDIS_SENTINEL_RECEIVE_JOIN) {
        reply = (redisReply*)r;

        if (reply == NULL)
            goto error;

        if (reply->type == REDIS_REPLY_NIL) {
            redisLog(REDIS_WARNING, "Sentinel %s:%d told me nothing.", sentinel->host, sentinel->port);
            goto error;
        }


        if (reply->type == REDIS_REPLY_ERROR || (reply->type == REDIS_REPLY_STATUS && strstr(reply->str, "PENDING") != NULL)) {                        
            server.sentinel_conn_state = REDIS_SENTINEL_RETRY_JOIN;            
            return;
        }
        
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 1) {
            redisLog(REDIS_WARNING, "Expected array with (type) or (type,host,port) from sentinel %s:%d, got %s instead.", 
                    sentinel->host,
                    sentinel->port,
                    reply->str);

            goto error;
        }

        char *type = reply->element[0]->str;
        if (strcasecmp(type, "master") == 0) {
            // Sentinel appointed me as master
            becomeMaster();
            redisLog(REDIS_NOTICE, "I am now the master of sentinel group %s", server.sentinel_master_name);
        } else if (strcasecmp(type, "slave") == 0 && reply->elements == 3) {
            char *host = reply->element[1]->str;
            long port = atol(reply->element[2]->str);            

            if (isSelfAddr(host, port) == 0) {
                becomeMaster();
                redisLog(REDIS_WARNING, "Sentinels consider me slave, but the adress of master is myself, so i'm master of group %s", server.sentinel_master_name);
            } else if (server.repl_state != REDIS_REPL_NONE &&
                server.masterhost != NULL && 
                strcmp(server.masterhost, host)==0 && 
                port==server.masterport) 
            {
                redisLog(REDIS_NOTICE, "Our current master is in sync with sentinel %s:%d state.", sentinel->host, sentinel->port);
            } else {
                redisLog(REDIS_NOTICE, "Master for sentinel group %s is %s:%d", server.sentinel_master_name, host, port);

                sdsfree(server.masterhost);
                server.masterhost = sdsnew(host);
                server.masterport = port;

                disconnectSlaves(); // Force our slaves to resync with us as well. 
                if (server.repl_state == REDIS_REPL_TRANSFER)
                    replicationAbortSyncTransfer();                

                server.repl_state = REDIS_REPL_CONNECT;

                /* If we're unable to connect to this master, we should query the sentinel again before retrying
                   in case sentinel decided to fail-over to a new master. */
                server.repl_reconnect_using_sentinel = 1; 

                redisLog(REDIS_NOTICE,"SLAVE OF %s:%d enabled (sentinel-initiated)",
                        server.masterhost, server.masterport);
            }
        }
        
        server.sentinel_conn_state = REDIS_SENTINEL_NONE;
        if (server.sentinel_conn) {
            redisAsyncFree(server.sentinel_conn);
            server.sentinel_conn = NULL;
        }
        sentinel->status = REDIS_OK;
        return;
    }

    return;

error:
    redisAsyncFree(server.sentinel_conn);
    sentinel->status = REDIS_ERR;
    server.sentinel_conn = NULL;
    server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;

    return;
}

void syncWithSentinelDisconnected(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK && server.sentinel_conn_state != REDIS_SENTINEL_NONE) {
        server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
        server.sentinel_conn = NULL;
    }
}

void syncWithSentinelConnected(const redisAsyncContext *c, int status) {
    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    if (server.sentinel_conn_state == REDIS_SENTINEL_NONE) {
        redisAsyncFree(server.sentinel_conn);
        server.sentinel_conn = NULL;
        return;
    }

    if (status != REDIS_OK) {
        redisLog(REDIS_WARNING,"Error condition on socket for sentinel group join: %s", c->errstr);
        goto error;
    }

    if (server.sentinel_conn_state == REDIS_SENTINEL_CONNECTING) {
        server.repl_sentinel_last_io = time(NULL);
        struct sockaddr_in sa;
        socklen_t salen = sizeof(sa);
        
        if (getsockname(c->c.fd,(struct sockaddr*)&sa,&salen) != -1) {            
            redisAsyncCommand(server.sentinel_conn, 
                      syncWithSentinelJoinGroupCallback, 
                      NULL, 
                      "sentinel group join %s %s %d", 
                      server.sentinel_master_name, inet_ntoa(sa.sin_addr), server.port);
            server.sentinel_conn_state = REDIS_SENTINEL_RECEIVE_JOIN;
        }
    }

    return;

error:
    ((sentinelInfo*)(c->data))->status = REDIS_ERR;

    server.sentinel_conn = NULL;
    
    server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
}

void syncWithMaster(aeEventLoop *el, int fd, void *privdata, int mask) {
    char tmpfile[256], *err;
    int dfd, maxtries = 5;
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    if (server.repl_state == REDIS_REPL_NONE) {
        close(fd);
        return;
    }

    /* Check for errors in the socket. */
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    if (sockerr) {
        aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
        redisLog(REDIS_WARNING,"Error condition on socket for SYNC: %s",
            strerror(sockerr));
        goto error;
    }

    /* If we were connecting, it's time to send a non blocking PING, we want to
     * make sure the master is able to reply before going into the actual
     * replication process where we have long timeouts in the order of
     * seconds (in the meantime the slave would block). */
    if (server.repl_state == REDIS_REPL_CONNECTING) {
        redisLog(REDIS_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        aeDeleteFileEvent(server.el,fd,AE_WRITABLE);
        server.repl_state = REDIS_REPL_RECEIVE_PONG;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        syncWrite(fd,"PING\r\n",6,100);
        return;
    }

    /* Receive the PONG command. */
    if (server.repl_state == REDIS_REPL_RECEIVE_PONG) {
        char buf[1024];

        /* Delete the readable event, we no longer need it now that there is
         * the PING reply to read. */
        aeDeleteFileEvent(server.el,fd,AE_READABLE);

        /* Read the reply with explicit timeout. */
        buf[0] = '\0';
        if (syncReadLine(fd,buf,sizeof(buf),
            server.repl_syncio_timeout*1000) == -1)
        {
            redisLog(REDIS_WARNING,
                "I/O error reading PING reply from master: %s",
                strerror(errno));
            goto error;
        }

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        if (buf[0] != '+' &&
            strncmp(buf,"-NOAUTH",7) != 0 &&
            strncmp(buf,"-ERR operation not permitted",28) != 0)
        {
            redisLog(REDIS_WARNING,"Error reply to PING from master: '%s'",buf);
            goto error;
        } else {
            redisLog(REDIS_NOTICE,
                "Master replied to PING, replication can continue...");
        }
    }

    /* AUTH with the master if required. */
    if(server.masterauth) {
        err = sendSynchronousCommand(fd,"AUTH",server.masterauth,NULL);
        if (err) {
            redisLog(REDIS_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
    }

    /* Set the slave port, so that Master's INFO command can list the
     * slave listening port correctly. */
    {
        sds port = sdsfromlonglong(server.port);
        err = sendSynchronousCommand(fd,"REPLCONF","listening-port",port,
                                         NULL);
        sdsfree(port);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err) {
            redisLog(REDIS_NOTICE,"(non critical): Master does not understand REPLCONF listening-port: %s", err);
            sdsfree(err);
        }
    }

    /* Issue the SYNC command */
    if (syncWrite(fd,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
        redisLog(REDIS_WARNING,"I/O error writing to MASTER: %s",
            strerror(errno));
        goto error;
    }

    /* Prepare a suitable temp file for bulk transfer */
    while(maxtries--) {
        snprintf(tmpfile,256,
            "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
        dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        redisLog(REDIS_WARNING,"Opening the temp file needed for MASTER <-> SLAVE synchronization: %s",strerror(errno));
        goto error;
    }

    /* Setup the non blocking download of the bulk file. */
    if (aeCreateFileEvent(server.el,fd, AE_READABLE,readSyncBulkPayload,NULL)
            == AE_ERR)
    {
        redisLog(REDIS_WARNING,
            "Can't create readable event for SYNC: %s (fd=%d)",
            strerror(errno),fd);
        goto error;
    }

    server.repl_reconnect_using_sentinel = 0; 
    server.repl_state = REDIS_REPL_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_fd = dfd;
    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_tmpfile = zstrdup(tmpfile);
    return;

error:
    close(fd);
    server.repl_transfer_s = -1;

    if (server.repl_reconnect_using_sentinel) {
        /* Master connection failed, ask sentinel for a new master */
        server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
        server.repl_state = REDIS_REPL_NONE;
    }
    else
        server.repl_state = REDIS_REPL_CONNECT;

    return;
}

int connectWithSentinel(void) {
    listNode *node = NULL;
    sentinelInfo *sentinel = NULL;
   
    if (server.sentinels == NULL) {
        redisLog(REDIS_WARNING, "Unable to connect to sentinel because the list is NULL.");
        return REDIS_ERR;
    }

    if (server.sentinel_iterator == NULL) {
        server.sentinel_iterator = listGetIterator(server.sentinels, AL_START_HEAD);
    }

        
    node = server.sentinel_iterator->next;   

    sentinel = node ? (sentinelInfo*)node->value : NULL;

    /* If we failed to connect to this sentinel, move on to the next one */
    if (sentinel && sentinel->status == REDIS_ERR) {
        sentinel->status = REDIS_OK;

        listNext(server.sentinel_iterator);
        node = server.sentinel_iterator->next;
    }

    if (node == NULL) {
        listRewind(server.sentinels, server.sentinel_iterator);
        node = listNext(server.sentinel_iterator);

        if (node == NULL) {
            redisLog(REDIS_WARNING, "Unable to connect to sentinel because the list is empty.");
            return REDIS_ERR;
        }
    }

    sentinel = (sentinelInfo*)node->value;

    redisLog(REDIS_NOTICE, "Connecting to sentinel %s:%d...", sentinel->host, sentinel->port);
    redisAsyncContext *c = redisAsyncConnect(sentinel->host, sentinel->port);

    if (c->err) {
        redisLog(REDIS_WARNING,"Unable to connect to sentinel: %s",
                 c->errstr);
    
        redisAsyncFree(c);
        return REDIS_ERR;
    }

    server.repl_sentinel_last_io = time(NULL);

    c->data = sentinel;

    redisAeAttach(server.el, c);
    redisAsyncSetConnectCallback(c, syncWithSentinelConnected);
    redisAsyncSetDisconnectCallback(c, syncWithSentinelDisconnected);
    
    server.sentinel_conn_state = REDIS_SENTINEL_CONNECTING;    
    server.sentinel_conn = c;

    return REDIS_OK;
}

int connectWithMaster(void) {
    int fd;

    fd = anetTcpNonBlockConnect(NULL,server.masterhost,server.masterport);
    if (fd == -1) {
        redisLog(REDIS_WARNING,"Unable to connect to MASTER: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    
    if (aeCreateFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE,syncWithMaster,NULL) ==
            AE_ERR)
    {
        close(fd);
        redisLog(REDIS_WARNING,"Can't create readable event for SYNC");
        return REDIS_ERR;
    }

    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_s = fd;
    server.repl_state = REDIS_REPL_CONNECTING;
    return REDIS_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it. */
void undoConnectWithMaster(void) {
    int fd = server.repl_transfer_s;

    redisAssert(server.repl_state == REDIS_REPL_CONNECTING ||
                server.repl_state == REDIS_REPL_RECEIVE_PONG);
    aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
    close(fd);
    server.repl_transfer_s = -1;

    /* If this connection was initiated by sentinel, re-connect to sentinel and find out if we have a new master */
    if (server.repl_reconnect_using_sentinel)
    {
        server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
        server.repl_state = REDIS_REPL_NONE;
    }
    else
        server.repl_state = REDIS_REPL_CONNECT;
}

void undoConnectWithSentinel(void) {
    if (server.sentinel_conn) {
        redisAsyncFree(server.sentinel_conn);
        server.sentinel_conn = NULL;
    }
    
    server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
}

void sentinelsCommand(redisClient *c) {
    int j=1;
    
    if (strcasecmp(c->argv[1]->ptr, "no")==0 && strcasecmp(c->argv[2]->ptr, "one")==0) {
        undoConnectWithSentinel();
        server.repl_reconnect_using_sentinel = 0;
        server.sentinel_conn_state = REDIS_SENTINEL_NONE;

        addReply(c,shared.ok);
        return;
    }

    if (((c->argc-1) % 2) == 0) {
        addReplyError(c,"wrong number of arguments");
        return;
    }

    // Free the old sentinel list and it's iterator
    if (server.sentinels != NULL) {
        listIter* iter = listGetIterator(server.sentinels, AL_START_HEAD);

        if (iter != NULL) {
            listNode *node = NULL;

            while ((node = listNext(iter)) != NULL) {
                sentinelInfo *s = (sentinelInfo*)node->value;

                sdsfree(s->host);
            }
        }

        listReleaseIterator(server.sentinel_iterator);
        listRelease(server.sentinels);
        
        server.sentinels = NULL;
        server.sentinel_iterator = NULL;
    }

    server.sentinel_conn_state = REDIS_SENTINEL_NONE;
    sdsfree(server.sentinel_master_name);
    server.sentinel_master_name = NULL;
    server.sentinel_master_name = sdsnew(c->argv[1]->ptr);

    for (j=2; j < c->argc-1; j+=2) {
    
        long port = 0;

        if ((getLongFromObjectOrReply(c, c->argv[j+1], &port, NULL) != REDIS_OK))
            break;

        if (server.sentinels == NULL) {
            server.sentinels = listCreate();
        }

        sentinelInfo *sentinel = (sentinelInfo*)zmalloc(sizeof(sentinelInfo));

        if (!sentinel) {
            redisLog(REDIS_WARNING, "Out of memory!");
            return;
        }

        sentinel->host = sdsnew(c->argv[j]->ptr);
        sentinel->port = (int)port;

        if (listAddNodeTail(server.sentinels, sentinel) == NULL) {
            redisLog(REDIS_WARNING, "Failed to add sentinel to adlist");

            sdsfree(sentinel->host);
            zfree(sentinel);

            break;
        }
    
        redisLog(REDIS_NOTICE,"Now following sentinel %s:%d",
            sentinel->host, sentinel->port);
        
        if (server.sentinel_conn_state == REDIS_SENTINEL_NONE) 
            server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
    }

    addReply(c,shared.ok);
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REDIS_REPL_CONNECT.
 *
 * Otherwise zero is returned and no operation is perforemd at all. */
int cancelReplicationHandshake(void) {
    if (server.repl_state == REDIS_REPL_TRANSFER) {
        replicationAbortSyncTransfer();
    } else if (server.repl_state == REDIS_REPL_CONNECTING ||
             server.repl_state == REDIS_REPL_RECEIVE_PONG)
    {
        undoConnectWithMaster();
    } else {
        return 0;
    }
    return 1;
}

void slaveofCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) 
    {
        becomeMaster();
        redisLog(REDIS_NOTICE,"MASTER MODE enabled (user request)");
    } else {
        long port;

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != REDIS_OK))
            return;

        if (isSlaveOf(c->argv[1]->ptr, port) == 0) {
            redisLog(REDIS_NOTICE,"SLAVE OF would result into synchronization with the master we are already connected with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified master\r\n"));
            return;
        }

        if (isSelfAddr(c->argv[1]->ptr, port) == 0) {
            becomeMaster();
            redisLog(REDIS_WARNING, "Unable to sync with myself");
            redisLog(REDIS_NOTICE,"MASTER MODE enabled (user request)");
            addReplySds(c,sdsnew("+OK Can't sync with myself => i'm now master\r\n"));
            return;
        }

        /* There was no previous master or the user specified a different one,
         * we can continue. */
        sdsfree(server.masterhost);
        server.masterhost = sdsdup(c->argv[1]->ptr);
        server.masterport = port;
        server.repl_reconnect_using_sentinel = 0;
        if (server.master) freeClient(server.master);
        disconnectSlaves(); /* Force our slaves to resync with us as well. */
        cancelReplicationHandshake();
        server.repl_state = REDIS_REPL_CONNECT;
        redisLog(REDIS_NOTICE,"SLAVE OF %s:%d enabled (user request)",
            server.masterhost, server.masterport);
    }
    addReply(c,shared.ok);
}

int isSlaveOf(const char* host, int port) {
    /* Check if we are already attached to the specified slave */
    if (server.masterhost && !strcasecmp(server.masterhost, host)
        && server.masterport == port)
    {
        return 0;
    }
    return -1;
}

void becomeMaster(void) {
    if (server.sentinel_conn_state != REDIS_SENTINEL_NONE) {
        undoConnectWithSentinel();
        server.sentinel_conn_state = REDIS_SENTINEL_NONE;
    }

    if (server.masterhost) {
        sdsfree(server.masterhost);
        server.masterhost = NULL;
        if (server.master) freeClient(server.master);
        cancelReplicationHandshake();
        server.repl_state = REDIS_REPL_NONE;
        server.repl_reconnect_using_sentinel = 0;
    }
}

int isLocalHost(const char* checkHost) {
    struct ifaddrs *ifaddr, *ifa;
    int family, s;
    char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1) {
        redisLog(REDIS_WARNING, "Failed to get local addresses");
        return -1;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        family = ifa->ifa_addr->sa_family;

        if (family == AF_INET || family == AF_INET6) {
            s = getnameinfo(ifa->ifa_addr,
                    (family == AF_INET) ? sizeof(struct sockaddr_in) :
                                          sizeof(struct sockaddr_in6),
                    host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                continue;
            }
            redisLog(REDIS_WARNING, "<%s> <%s>\n", checkHost, host);
            if (strcasecmp(checkHost, host) == 0) {
                return 0;
            }
        }
    }
    return -1;
}

int isSelfAddr(const char* host, int port) {
    struct hostent* hostEnt = gethostbyname(host);
    if (isLocalHost(inet_ntoa(*(struct in_addr*)hostEnt->h_addr_list[0])) == 0 &&
        server.port == port)
    {
        return 0;
    }
    return -1;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

void replicationCron(void) {
    /* Non blocking connection timeout? */
    if (server.masterhost &&
        (server.repl_state == REDIS_REPL_CONNECTING ||
         server.repl_state == REDIS_REPL_RECEIVE_PONG) &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout connecting to the MASTER...");
        undoConnectWithMaster();
    }

    /* Connect to sentinel timeout? */
    if (server.sentinels && (server.sentinel_conn_state == REDIS_SENTINEL_CONNECTING ||
        server.sentinel_conn_state == REDIS_SENTINEL_RECEIVE_JOIN) &&
        (time(NULL)-server.repl_sentinel_last_io > server.repl_timeout))
    {
        redisLog(REDIS_WARNING, "Timeout waiting for data from sentinel... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        undoConnectWithSentinel();
    }
    
    /* Retry join sentinel group? */    
    if (server.sentinels && (server.sentinel_conn_state == REDIS_SENTINEL_RETRY_JOIN &&
        (time(NULL)-server.repl_sentinel_last_io >= (rand() % 3) + 1)))
    {
        redisLog(REDIS_WARNING, "Retrying to join sentinel group %s", server.sentinel_master_name);
        server.sentinel_conn_state = REDIS_SENTINEL_CONNECTING;
        syncWithSentinelConnected(server.sentinel_conn, REDIS_OK);
    }

    /* Bulk transfer I/O timeout? */
    if (server.masterhost && server.repl_state == REDIS_REPL_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        replicationAbortSyncTransfer();
    }

    /* Timed out master when we are an already connected slave? */
    if (server.masterhost && server.repl_state == REDIS_REPL_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"MASTER time out: no data nor PING received...");
        freeClient(server.master);
    }

    /* Check if we should follow a sentinel */
    if (server.sentinel_conn_state == REDIS_SENTINEL_CONNECT) {
        redisLog(REDIS_NOTICE, "Following a sentinel...");
        if (connectWithSentinel() == REDIS_OK) {
            redisLog(REDIS_NOTICE, "Sentinel sync started");
        }
    }
    /* Check if we should connect to a MASTER */
    if (server.repl_state == REDIS_REPL_CONNECT) {
        redisLog(REDIS_NOTICE,"Connecting to MASTER...");
        if (connectWithMaster() == REDIS_OK) {
            redisLog(REDIS_NOTICE,"MASTER <-> SLAVE sync started");
        }
    }
    
    /* If we have attached slaves, PING them from time to time.
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    if (!(server.cronloops % (server.repl_ping_slave_period * server.hz))) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;

            /* Don't ping slaves that are in the middle of a bulk transfer
             * with the master for first synchronization. */
            if (slave->replstate == REDIS_REPL_SEND_BULK) continue;
            if (slave->replstate == REDIS_REPL_ONLINE) {
                /* If the slave is online send a normal ping */
                addReplySds(slave,sdsnew("*1\r\n$4\r\nPING\r\n"));
            } else {
                /* Otherwise we are in the pre-synchronization stage.
                 * Just a newline will do the work of refreshing the
                 * connection last interaction time, and at the same time
                 * we'll be sure that being a single char there are no
                 * short-write problems. */
                if (write(slave->fd, "\n", 1) == -1) {
                    /* Don't worry, it's just a ping. */
                }
            }
        }
    }
}

void syncWithSentinels(void) {
    time_t current = time(NULL);
    if ((!server.masterhost || server.repl_sentinel_next_update < current) &&
        server.sentinel_conn_state == REDIS_SENTINEL_NONE)
    {
        server.sentinel_conn_state = REDIS_SENTINEL_CONNECT;
        /* Next sentinels polling for slave will be in (15 - 45 minutes)
         * for slaves it's only a backup mechanism if sentinels failed to
         * swithc slaves themself */
        server.repl_sentinel_next_update = current + 54000 + rand() % 108000;
    }
}
