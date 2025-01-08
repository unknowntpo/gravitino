/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::config::AppConfig;
use crate::filesystem::{FileStat, FileSystemContext, RawFileSystem};
use crate::fuse_api_handle::FuseApiHandle;
use chrono::{DateTime, NaiveDateTime, Utc};
use fuse3::path::prelude::{ReplyData, ReplyOpen, ReplyStatFs, ReplyWrite};
use fuse3::path::Request;
use fuse3::raw::prelude::{
    FileAttr, ReplyAttr, ReplyCreated, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit,
};
use fuse3::raw::reply::{DirectoryEntry, DirectoryEntryPlus};
use fuse3::raw::Filesystem;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType, Inode, SetAttr, Timestamp};
use futures_util::stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};
use tokio::fs::remove_dir_all;
use tracing::{debug, error};
use tracing_subscriber::fmt::format;

pub struct FileAttrDebug<'a> {
    pub file_attr: &'a FileAttr,
}

impl<'a> std::fmt::Debug for FileAttrDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attr = &self.file_attr;
        let mut struc = f.debug_struct("FileAttr");

        struc
            .field("ino", &attr.ino)
            .field("size", &attr.size)
            .field("blocks", &attr.blocks)
            .field("atime", &timestamp_to_readable_time_string(attr.atime))
            .field("mtime", &timestamp_to_readable_time_string(attr.mtime))
            .field("ctime", &timestamp_to_readable_time_string(attr.ctime));

        // Conditionally add the "crtime" field only for macOS
        #[cfg(target_os = "macos")]
        {
            struc.field("crtime", &timestamp_to_readable_time_string(attr.crtime));
        }

        struc
            .field("kind", &attr.kind)
            .field("perm", &attr.perm)
            .field("nlink", &attr.nlink)
            .field("uid", &attr.uid)
            .field("gid", &attr.gid)
            .field("rdev", &attr.rdev)
            .finish()
    }
}

/// Example output: "2025-01-07 23:01:30.531699"
fn timestamp_to_readable_time_string(tstmp: Timestamp) -> String {
    DateTime::from_timestamp(tstmp.sec, tstmp.nsec)
        .unwrap()
        .naive_utc()
        .to_string()
}

pub(crate) struct FuseApiHandleDebug<T: RawFileSystem> {
    inner: FuseApiHandle<T>,
}

impl<T: RawFileSystem> FuseApiHandleDebug<T> {
    pub fn new(fs: T, _config: &AppConfig, context: FileSystemContext) -> Self {
        Self {
            inner: FuseApiHandle::new(fs, _config, context),
        }
    }
}

impl<T: RawFileSystem> Filesystem for FuseApiHandleDebug<T> {
    async fn init(&self, req: Request) -> fuse3::Result<ReplyInit> {
        debug!(req.unique, ?req, "init");
        match self.inner.init(req).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "init");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?req, "init");
                Err(e)
            }
        }
    }

    async fn destroy(&self, req: Request) {
        debug!(req.unique, ?req, "destroy started");
        self.inner.destroy(req).await;
        debug!(req.unique, "destroy completed");
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        debug!(req.unique, ?req, ?parent, ?name, "lookup started");
        match self.inner.lookup(req, parent, name).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "lookup completed");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "lookup failed");
                Err(e)
            }
        }
    }

    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        debug!(req.unique, ?req, ?inode, ?fh, ?flags, "getattr started");
        match self.inner.getattr(req, inode, fh, flags).await {
            Ok(reply) => {
                // FIXME: reply: ReplyAttr should be formatted in human readable way
                debug!(req.unique, ?reply, "getattr completed");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "getattr failed");
                Err(e)
            }
        }
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        debug!(req.unique, ?req, ?inode, ?fh, ?set_attr, "setattr started");
        match self.inner.setattr(req, inode, fh, set_attr).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "setattr completed");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "setattr failed");
                Err(e)
            }
        }
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        debug!(
            req.unique,
            ?req,
            ?parent,
            ?name,
            mode,
            umask,
            "mkdir started"
        );

        match self.inner.mkdir(req, parent, name, mode, umask).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "mkdir succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "mkdir failed");
                Err(e)
            }
        }
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        debug!(req.unique, ?req, ?parent, ?name, "unlink started");

        match self.inner.unlink(req, parent, name).await {
            Ok(()) => {
                debug!(req.unique, "unlink succeeded");
                Ok(())
            }
            Err(e) => {
                error!(req.unique, ?e, "unlink failed");
                Err(e)
            }
        }
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        debug!(req.unique, ?req, ?parent, ?name, "rmdir started");

        match self.inner.rmdir(req, parent, name).await {
            Ok(()) => {
                debug!(req.unique, "rmdir succeeded");
                Ok(())
            }
            Err(e) => {
                error!(req.unique, ?e, "rmdir failed");
                Err(e)
            }
        }
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        debug!(req.unique, ?req, ?inode, ?flags, "open started");

        match self.inner.open(req, inode, flags).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "open succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "open failed");
                Err(e)
            }
        }
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        debug!(
            req.unique,
            ?req,
            ?inode,
            ?fh,
            ?offset,
            ?size,
            "read started"
        );

        match self.inner.read(req, inode, fh, offset, size).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "read succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "read failed");
                Err(e)
            }
        }
    }

    async fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        debug!(
            req.unique,
            ?req,
            ?inode,
            ?fh,
            ?offset,
            data_len = data.len(),
            ?write_flags,
            ?flags,
            "write started"
        );

        match self
            .inner
            .write(req, inode, fh, offset, data, write_flags, flags)
            .await
        {
            Ok(reply) => {
                debug!(req.unique, ?reply, "write succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "write failed");
                Err(e)
            }
        }
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        debug!(req.unique, ?req, ?inode, "statfs started");

        match self.inner.statfs(req, inode).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "statfs succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "statfs failed");
                Err(e)
            }
        }
    }

    async fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> fuse3::Result<()> {
        debug!(
            req.unique,
            ?req,
            ?inode,
            ?fh,
            ?flags,
            ?lock_owner,
            ?flush,
            "release started"
        );

        match self
            .inner
            .release(req, inode, fh, flags, lock_owner, flush)
            .await
        {
            Ok(()) => {
                debug!(req.unique, "release succeeded");
                Ok(())
            }
            Err(e) => {
                error!(req.unique, ?e, "release failed");
                Err(e)
            }
        }
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        debug!(req.unique, ?req, ?inode, ?flags, "opendir started");

        match self.inner.opendir(req, inode, flags).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "opendir succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "opendir failed");
                Err(e)
            }
        }
    }

    type DirEntryStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntry>>
    where
        T: 'a;

    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            parent = ?stat.name,
            ?fh,
            ?offset,
            "readdir started"
        );

        match self.inner.readdir(req, parent, fh, offset).await {
            Ok(reply) => {
                debug!(req.unique, "readdir succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "readdir failed");
                Err(e)
            }
        }
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        debug!(
            "releasedir [req.unique={}]: req: {:?}, inode: {:?}, fh: {:?}, flags: {:?}",
            req.unique, req, inode, fh, flags
        );
        let result = self.inner.releasedir(req, inode, fh, flags).await;
        match result {
            Ok(()) => {
                debug!("releasedir [req.unique={}]: success", req.unique);
                Ok(())
            }
            Err(e) => {
                error!("releasedir [req.unique={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        debug!(
            req.unique,
            ?req,
            ?parent,
            ?name,
            ?mode,
            ?flags,
            "create started"
        );

        match self.inner.create(req, parent, name, mode, flags).await {
            Ok(reply) => {
                debug!(req.unique, ?reply, "create succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "create failed");
                Err(e)
            }
        }
    }

    type DirEntryPlusStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntryPlus>>
    where
        T: 'a;

    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> fuse3::Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        debug!(
            req.unique,
            ?req,
            ?parent,
            ?fh,
            ?offset,
            ?lock_owner,
            "readdirplus started"
        );

        match self
            .inner
            .readdirplus(req, parent, fh, offset, lock_owner)
            .await
        {
            Ok(reply) => {
                debug!(req.unique, "readdirplus succeeded");
                Ok(reply)
            }
            Err(e) => {
                error!(req.unique, ?e, "readdirplus failed");
                Err(e)
            }
        }
    }
}
