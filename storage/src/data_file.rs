use crate::{page::PAGE_SIZE, Page, PageId, Result, StorageError};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom},
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
        read_shared(&self.file, page_id)
    }

    /// A second handle onto the same file for positional reads.
    ///
    /// Positional reads never depend on the shared cursor, and every writer
    /// in this module seeks explicitly before writing, so readers on the
    /// shared handle cannot disturb them.
    pub(crate) fn share(&self) -> Result<File> {
        Ok(self.file.try_clone()?)
    }

    pub(crate) fn write(&mut self, page: &Page) -> Result<()> {
        // Positional writes only: the shared read handle moves the common
        // cursor on some platforms, so nothing here may depend on it.
        write_all_at(&self.file, &page.encode(), page_offset(page.id())?)
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

/// Read one page at its offset without using the shared cursor.
pub(crate) fn read_shared(file: &File, page_id: PageId) -> Result<Option<Page>> {
    let offset = page_offset(page_id)?;
    let length = file.metadata()?.len();
    if offset.saturating_add(PAGE_SIZE as u64) > length {
        return Ok(None);
    }

    let mut bytes = [0_u8; PAGE_SIZE];
    read_exact_at(file, &mut bytes, offset)?;
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

#[cfg(windows)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0_usize;
    while written < buffer.len() {
        let count = file.seek_write(&buffer[written..], offset + written as u64)?;
        if count == 0 {
            return Err(StorageError::CorruptDataFile(
                "page write made no progress".to_owned(),
            ));
        }
        written += count;
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, buffer: &[u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buffer, offset)?;
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;
    let mut filled = 0_usize;
    while filled < buffer.len() {
        let read = file.seek_read(&mut buffer[filled..], offset + filled as u64)?;
        if read == 0 {
            return Err(StorageError::CorruptDataFile(
                "page read ended before the page did".to_owned(),
            ));
        }
        filled += read;
    }
    Ok(())
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buffer, offset)?;
    Ok(())
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
