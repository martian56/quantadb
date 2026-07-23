use crate::{page::PAGE_SIZE, Page, PageId, Result, StorageError};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(crate) struct DataFile {
    file: File,
}

impl DataFile {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let length = file.metadata()?.len();
        let complete_length = length - (length % PAGE_SIZE as u64);
        if complete_length != length {
            // A partial final page can only be a torn append. WAL recovery will
            // recreate it if the write had become durable.
            file.set_len(complete_length)?;
            file.seek(SeekFrom::Start(complete_length))?;
        }
        Ok(Self { file })
    }

    pub(crate) fn read(&mut self, page_id: PageId) -> Result<Option<Page>> {
        let offset = page_offset(page_id)?;
        let length = self.file.metadata()?.len();
        if offset.saturating_add(PAGE_SIZE as u64) > length {
            return Ok(None);
        }

        self.file.seek(SeekFrom::Start(offset))?;
        let mut bytes = [0_u8; PAGE_SIZE];
        self.file.read_exact(&mut bytes)?;
        if bytes.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }
        let page = Page::decode(&bytes)?;
        if page.id() != page_id {
            return Err(StorageError::CorruptPage {
                page_id,
                reason: format!("page header contains ID {}", page.id()),
            });
        }
        Ok(Some(page))
    }

    pub(crate) fn write(&mut self, page: &Page) -> Result<()> {
        self.file.seek(SeekFrom::Start(page_offset(page.id())?))?;
        self.file.write_all(&page.encode())?;
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub(crate) fn page_count(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len() / PAGE_SIZE as u64)
    }

    pub(crate) fn max_lsn(&mut self) -> Result<u64> {
        let mut maximum = 0_u64;
        for raw_page_id in 0..self.page_count()? {
            if let Some(page) = self.read(PageId(raw_page_id))? {
                maximum = maximum.max(page.lsn().0);
            }
        }
        Ok(maximum)
    }
}

fn page_offset(page_id: PageId) -> Result<u64> {
    page_id
        .0
        .checked_mul(PAGE_SIZE as u64)
        .ok_or_else(|| StorageError::CorruptDataFile("page offset overflow".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sparse_pages_round_trip_and_holes_are_absent() {
        let directory = tempdir().expect("tempdir");
        let mut data = DataFile::open(&directory.path().join("data.qdb")).expect("open");
        let page = Page::new(PageId(4), b"four".to_vec()).expect("page");
        data.write(&page).expect("write");
        data.sync().expect("sync");

        assert_eq!(data.read(PageId(4)).expect("read"), Some(page));
        assert_eq!(data.read(PageId(2)).expect("read hole"), None);
    }
}
