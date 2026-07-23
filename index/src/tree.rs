use crate::{
    cache::NodeCache,
    node::{header_size, internal_entry_size, leaf_entry_size, Node},
    IndexError, Result,
};
use quantadb_storage::{GroupCommitHandle, PageId, PageWrite, MAX_PAGE_PAYLOAD};
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub value: PageId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexMutation {
    Upsert(IndexEntry),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexRoot {
    pub page_id: PageId,
    pub height: u16,
    pub entries: u64,
}

#[derive(Debug)]
pub struct IndexBuildPlan {
    root: Option<IndexRoot>,
    writes: Vec<PageWrite>,
}

impl IndexBuildPlan {
    #[must_use]
    pub const fn root(&self) -> Option<IndexRoot> {
        self.root
    }

    #[must_use]
    pub fn into_writes(self) -> Vec<PageWrite> {
        self.writes
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BPlusTree;

#[derive(Clone)]
struct Child {
    first_key: Vec<u8>,
    page_id: PageId,
}

struct CursorFrame {
    children: Vec<PageId>,
    position: usize,
    child_level: u16,
}

struct EditFrame {
    level: u16,
    children: Vec<Child>,
    position: usize,
}

struct Editor<'a> {
    storage: &'a GroupCommitHandle,
    cache: Option<&'a NodeCache>,
    root: Option<IndexRoot>,
    overlay: HashMap<PageId, Node>,
}

impl BPlusTree {
    /// Build and durably store one immutable index generation.
    ///
    /// Entries must be strictly ordered. Every generated node is committed in
    /// one atomic storage batch; the returned root is safe to publish.
    pub fn build(
        storage: &GroupCommitHandle,
        entries: impl IntoIterator<Item = IndexEntry>,
    ) -> Result<Option<IndexRoot>> {
        let plan = Self::plan(storage, entries)?;
        let root = plan.root();
        storage.commit(plan.into_writes())?;
        Ok(root)
    }

    /// Prepare an immutable generation without committing it.
    ///
    /// This lets a higher layer atomically include catalog or root metadata in
    /// the same storage batch as all index nodes.
    pub fn plan(
        storage: &GroupCommitHandle,
        entries: impl IntoIterator<Item = IndexEntry>,
    ) -> Result<IndexBuildPlan> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        validate_entries(&entries)?;
        if entries.is_empty() {
            return Ok(IndexBuildPlan {
                root: None,
                writes: Vec::new(),
            });
        }
        let entry_count =
            u64::try_from(entries.len()).map_err(|_| IndexError::EntryCountOverflow)?;
        let leaf_ranges = partition_leaves(&entries)?;
        let leaf_page_ids = storage.reserve_page_ids(leaf_ranges.len())?;
        let mut writes = Vec::new();
        let mut children = Vec::with_capacity(leaf_ranges.len());

        for (position, (range, page_id)) in leaf_ranges.iter().zip(&leaf_page_ids).enumerate() {
            let next = leaf_page_ids.get(position + 1).copied();
            let node = Node::Leaf {
                entries: entries[range.clone()].to_vec(),
                next,
            };
            writes.push(PageWrite {
                page_id: *page_id,
                payload: node.encode()?,
            });
            children.push(Child {
                first_key: entries[range.start].key.clone(),
                page_id: *page_id,
            });
        }

        let mut level = 0_u16;
        while children.len() > 1 {
            level = level.checked_add(1).ok_or(IndexError::EntryCountOverflow)?;
            let ranges = partition_internal(&children)?;
            let page_ids = storage.reserve_page_ids(ranges.len())?;
            let mut parents = Vec::with_capacity(ranges.len());

            for (range, page_id) in ranges.iter().zip(page_ids) {
                let group = &children[range.clone()];
                let node = Node::Internal {
                    level,
                    first_child: group[0].page_id,
                    separators: group[1..]
                        .iter()
                        .map(|child| (child.first_key.clone(), child.page_id))
                        .collect(),
                };
                writes.push(PageWrite {
                    page_id,
                    payload: node.encode()?,
                });
                parents.push(Child {
                    first_key: group[0].first_key.clone(),
                    page_id,
                });
            }
            children = parents;
        }

        let height = level.checked_add(1).ok_or(IndexError::EntryCountOverflow)?;
        Ok(IndexBuildPlan {
            root: Some(IndexRoot {
                page_id: children[0].page_id,
                height,
                entries: entry_count,
            }),
            writes,
        })
    }

    /// Apply ordered-key mutations with copy-on-write path updates.
    ///
    /// Existing nodes remain immutable. The returned plan contains only new
    /// nodes reachable from the final root, so intermediate roots created
    /// while applying several mutations are not persisted.
    pub fn edit_plan(
        storage: &GroupCommitHandle,
        cache: Option<&NodeCache>,
        root: Option<IndexRoot>,
        mutations: impl IntoIterator<Item = IndexMutation>,
    ) -> Result<IndexBuildPlan> {
        let mut editor = Editor {
            storage,
            cache,
            root,
            overlay: HashMap::new(),
        };
        for mutation in mutations {
            editor.apply(mutation)?;
        }
        editor.finish()
    }

    pub fn get(
        storage: &GroupCommitHandle,
        cache: Option<&NodeCache>,
        root: IndexRoot,
        key: &[u8],
    ) -> Result<Option<PageId>> {
        let mut page_id = root.page_id;
        let mut expected_level = root
            .height
            .checked_sub(1)
            .ok_or_else(|| corrupt(root.page_id, "root height cannot be zero"))?;

        loop {
            let node = read_node(storage, cache, page_id)?;
            if node.level() != expected_level {
                return Err(corrupt(
                    page_id,
                    format!(
                        "node level {} does not match expected level {expected_level}",
                        node.level()
                    ),
                ));
            }
            match &*node {
                Node::Leaf { entries, .. } => {
                    return Ok(entries
                        .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                        .ok()
                        .map(|position| entries[position].value));
                }
                Node::Internal {
                    first_child,
                    separators,
                    ..
                } => {
                    let child_position =
                        separators.partition_point(|(separator, _)| separator.as_slice() <= key);
                    page_id = if child_position == 0 {
                        *first_child
                    } else {
                        separators[child_position - 1].1
                    };
                    expected_level = expected_level
                        .checked_sub(1)
                        .ok_or_else(|| corrupt(page_id, "internal node below leaf level"))?;
                }
            }
        }
    }

    /// Scan `[start, end)` in key order, returning at most `limit` entries.
    pub fn range(
        storage: &GroupCommitHandle,
        cache: Option<&NodeCache>,
        root: IndexRoot,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<IndexEntry>> {
        if limit == 0 || start.zip(end).is_some_and(|(start, end)| start >= end) {
            return Ok(Vec::new());
        }

        let mut visited = HashSet::new();
        let mut frames = Vec::new();
        let (mut page_id, _) = descend_to_leaf(
            storage,
            cache,
            root.page_id,
            root.height
                .checked_sub(1)
                .ok_or_else(|| corrupt(root.page_id, "root height cannot be zero"))?,
            Some(start.unwrap_or_default()),
            &mut frames,
            &mut visited,
        )?;
        let mut result = Vec::new();
        let mut previous_key: Option<Vec<u8>> = None;

        while result.len() < limit {
            let node = read_node(storage, cache, page_id)?;
            let Node::Leaf { entries, .. } = &*node else {
                return Err(corrupt(page_id, "tree cursor reached an internal node"));
            };

            for entry in entries {
                if start.is_some_and(|start| entry.key.as_slice() < start) {
                    continue;
                }
                if end.is_some_and(|end| entry.key.as_slice() >= end) {
                    return Ok(result);
                }
                if previous_key
                    .as_deref()
                    .is_some_and(|previous| previous >= entry.key.as_slice())
                {
                    return Err(corrupt(page_id, "leaf chain is not strictly ordered"));
                }
                previous_key = Some(entry.key.clone());
                result.push(entry.clone());
                if result.len() == limit {
                    return Ok(result);
                }
            }

            let Some(next) = advance_to_next_leaf(storage, cache, &mut frames, &mut visited)?
            else {
                break;
            };
            page_id = next;
        }
        Ok(result)
    }
}

impl Editor<'_> {
    fn apply(&mut self, mutation: IndexMutation) -> Result<()> {
        let (key, value) = match mutation {
            IndexMutation::Upsert(entry) => {
                validate_entries(std::slice::from_ref(&entry))?;
                (entry.key, Some(entry.value))
            }
            IndexMutation::Delete(key) => (key, None),
        };

        let Some(root) = self.root else {
            if let Some(value) = value {
                let page_id = self.allocate(Node::Leaf {
                    entries: vec![IndexEntry { key, value }],
                    next: None,
                })?;
                self.root = Some(IndexRoot {
                    page_id,
                    height: 1,
                    entries: 1,
                });
            }
            return Ok(());
        };

        let mut page_id = root.page_id;
        let mut expected_level = root
            .height
            .checked_sub(1)
            .ok_or_else(|| corrupt(root.page_id, "root height cannot be zero"))?;
        let mut first_key = self.first_key(page_id)?;
        let mut frames = Vec::new();
        let mut leaf_entries;

        loop {
            let node = self.read(page_id)?;
            if node.level() != expected_level {
                return Err(corrupt(page_id, "node level does not match tree height"));
            }
            match node {
                Node::Leaf { entries, .. } => {
                    leaf_entries = entries;
                    break;
                }
                Node::Internal {
                    level,
                    first_child,
                    separators,
                } => {
                    let mut children = Vec::with_capacity(separators.len() + 1);
                    children.push(Child {
                        first_key,
                        page_id: first_child,
                    });
                    children.extend(
                        separators
                            .into_iter()
                            .map(|(first_key, page_id)| Child { first_key, page_id }),
                    );
                    let position = children
                        .partition_point(|child| child.first_key.as_slice() <= key.as_slice())
                        .saturating_sub(1);
                    page_id = children[position].page_id;
                    first_key = children[position].first_key.clone();
                    expected_level = expected_level
                        .checked_sub(1)
                        .ok_or_else(|| corrupt(page_id, "internal node below leaf level"))?;
                    frames.push(EditFrame {
                        level,
                        children,
                        position,
                    });
                }
            }
        }

        let position =
            leaf_entries.binary_search_by(|entry| entry.key.as_slice().cmp(key.as_slice()));
        let entry_delta = match (position, value) {
            (Ok(position), Some(value)) => {
                leaf_entries[position].value = value;
                0_i8
            }
            (Err(position), Some(value)) => {
                leaf_entries.insert(position, IndexEntry { key, value });
                1
            }
            (Ok(position), None) => {
                leaf_entries.remove(position);
                -1
            }
            (Err(_), None) => return Ok(()),
        };

        let mut replacement = self.make_leaves(&leaf_entries)?;
        let mut height = root.height;
        while let Some(mut frame) = frames.pop() {
            frame
                .children
                .splice(frame.position..frame.position + 1, replacement);
            let is_root = frames.is_empty();
            replacement = if frame.children.is_empty() {
                Vec::new()
            } else if is_root && frame.children.len() == 1 {
                height = height
                    .checked_sub(1)
                    .ok_or_else(|| corrupt(root.page_id, "tree height underflow"))?;
                frame.children
            } else {
                self.make_internal_level(frame.level, &frame.children)?
            };
        }

        let entries = match entry_delta {
            -1 => root
                .entries
                .checked_sub(1)
                .ok_or_else(|| corrupt(root.page_id, "root entry count underflow"))?,
            0 => root.entries,
            1 => root
                .entries
                .checked_add(1)
                .ok_or(IndexError::EntryCountOverflow)?,
            _ => unreachable!("entry delta is constrained above"),
        };
        self.root = match replacement.len() {
            0 => None,
            1 => Some(IndexRoot {
                page_id: replacement[0].page_id,
                height,
                entries,
            }),
            _ => {
                let level = height;
                let node = Node::Internal {
                    level,
                    first_child: replacement[0].page_id,
                    separators: replacement[1..]
                        .iter()
                        .map(|child| (child.first_key.clone(), child.page_id))
                        .collect(),
                };
                let page_id = self.allocate(node)?;
                Some(IndexRoot {
                    page_id,
                    height: height
                        .checked_add(1)
                        .ok_or(IndexError::EntryCountOverflow)?,
                    entries,
                })
            }
        };
        Ok(())
    }

    fn make_leaves(&mut self, entries: &[IndexEntry]) -> Result<Vec<Child>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        partition_leaves(entries)?
            .into_iter()
            .map(|range| {
                let first_key = entries[range.start].key.clone();
                let page_id = self.allocate(Node::Leaf {
                    entries: entries[range].to_vec(),
                    next: None,
                })?;
                Ok(Child { first_key, page_id })
            })
            .collect()
    }

    fn make_internal_level(&mut self, level: u16, children: &[Child]) -> Result<Vec<Child>> {
        partition_internal(children)?
            .into_iter()
            .map(|range| {
                let group = &children[range];
                let first_key = group[0].first_key.clone();
                let page_id = self.allocate(Node::Internal {
                    level,
                    first_child: group[0].page_id,
                    separators: group[1..]
                        .iter()
                        .map(|child| (child.first_key.clone(), child.page_id))
                        .collect(),
                })?;
                Ok(Child { first_key, page_id })
            })
            .collect()
    }

    fn first_key(&self, mut page_id: PageId) -> Result<Vec<u8>> {
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(page_id) {
                return Err(corrupt(page_id, "index tree contains a page cycle"));
            }
            match self.read(page_id)? {
                Node::Leaf { entries, .. } => {
                    return entries
                        .first()
                        .map(|entry| entry.key.clone())
                        .ok_or_else(|| corrupt(page_id, "reachable leaf is empty"));
                }
                Node::Internal { first_child, .. } => page_id = first_child,
            }
        }
    }

    fn allocate(&mut self, node: Node) -> Result<PageId> {
        let page_id = self
            .storage
            .reserve_page_ids(1)?
            .into_iter()
            .next()
            .ok_or(IndexError::EntryCountOverflow)?;
        self.overlay.insert(page_id, node);
        Ok(page_id)
    }

    fn read(&self, page_id: PageId) -> Result<Node> {
        if let Some(node) = self.overlay.get(&page_id) {
            return Ok(node.clone());
        }
        Ok(read_node(self.storage, self.cache, page_id)?.as_ref().clone())
    }

    fn finish(self) -> Result<IndexBuildPlan> {
        let mut reachable = HashSet::new();
        let mut pending = self.root.map(|root| vec![root.page_id]).unwrap_or_default();

        while let Some(page_id) = pending.pop() {
            if !reachable.insert(page_id) {
                return Err(corrupt(page_id, "copy-on-write plan contains a page cycle"));
            }
            let Some(node) = self.overlay.get(&page_id) else {
                continue;
            };
            if let Node::Internal {
                first_child,
                separators,
                ..
            } = node
            {
                pending.push(*first_child);
                pending.extend(separators.iter().map(|(_, child)| *child));
            }
        }

        let mut page_ids = reachable
            .into_iter()
            .filter(|page_id| self.overlay.contains_key(page_id))
            .collect::<Vec<_>>();
        page_ids.sort_unstable();
        let writes = page_ids
            .into_iter()
            .map(|page_id| {
                Ok(PageWrite {
                    page_id,
                    payload: self.overlay[&page_id].encode()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(IndexBuildPlan {
            root: self.root,
            writes,
        })
    }
}

fn validate_entries(entries: &[IndexEntry]) -> Result<()> {
    for pair in entries.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(IndexError::KeysNotStrictlyIncreasing);
        }
    }
    for entry in entries {
        let Some(entry_size) = leaf_entry_size(entry.key.len()) else {
            return Err(IndexError::KeyTooLarge {
                actual: usize::MAX,
                maximum: MAX_PAGE_PAYLOAD - header_size(),
            });
        };
        if header_size() + entry_size > MAX_PAGE_PAYLOAD {
            return Err(IndexError::KeyTooLarge {
                actual: entry.key.len(),
                maximum: MAX_PAGE_PAYLOAD - header_size() - 12,
            });
        }
    }
    Ok(())
}

fn partition_leaves(entries: &[IndexEntry]) -> Result<Vec<Range<usize>>> {
    partition_by(entries.len(), |position| {
        leaf_entry_size(entries[position].key.len())
    })
}

fn partition_internal(children: &[Child]) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < children.len() {
        let mut end = start + 1;
        let mut used = header_size();
        while end < children.len() {
            let size = internal_entry_size(children[end].first_key.len()).ok_or(
                IndexError::KeyTooLarge {
                    actual: usize::MAX,
                    maximum: MAX_PAGE_PAYLOAD - header_size(),
                },
            )?;
            if used + size > MAX_PAGE_PAYLOAD {
                break;
            }
            used += size;
            end += 1;
        }
        if end == start + 1 && end < children.len() {
            let key_length = children[end].first_key.len();
            let size = internal_entry_size(key_length).unwrap_or(usize::MAX);
            if header_size().saturating_add(size) > MAX_PAGE_PAYLOAD {
                return Err(IndexError::KeyTooLarge {
                    actual: key_length,
                    maximum: MAX_PAGE_PAYLOAD - header_size() - 12,
                });
            }
        }
        ranges.push(start..end);
        start = end;
    }
    Ok(ranges)
}

fn partition_by(
    length: usize,
    entry_size: impl Fn(usize) -> Option<usize>,
) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < length {
        let mut end = start;
        let mut used = header_size();
        while end < length {
            let size = entry_size(end).ok_or(IndexError::KeyTooLarge {
                actual: usize::MAX,
                maximum: MAX_PAGE_PAYLOAD - header_size(),
            })?;
            if used + size > MAX_PAGE_PAYLOAD {
                break;
            }
            used += size;
            end += 1;
        }
        if end == start {
            return Err(IndexError::KeyTooLarge {
                actual: entry_size(start).unwrap_or(usize::MAX),
                maximum: MAX_PAGE_PAYLOAD - header_size(),
            });
        }
        ranges.push(start..end);
        start = end;
    }
    Ok(ranges)
}

fn descend_to_leaf(
    storage: &GroupCommitHandle,
    cache: Option<&NodeCache>,
    mut page_id: PageId,
    mut expected_level: u16,
    key: Option<&[u8]>,
    frames: &mut Vec<CursorFrame>,
    visited: &mut HashSet<PageId>,
) -> Result<(PageId, u16)> {
    loop {
        if !visited.insert(page_id) {
            return Err(corrupt(page_id, "index tree contains a page cycle"));
        }
        let node = read_node(storage, cache, page_id)?;
        if node.level() != expected_level {
            return Err(corrupt(page_id, "node level does not match tree height"));
        }
        match &*node {
            Node::Leaf { .. } => return Ok((page_id, expected_level)),
            Node::Internal {
                first_child,
                separators,
                ..
            } => {
                let mut children = Vec::with_capacity(separators.len() + 1);
                children.push(*first_child);
                children.extend(separators.iter().map(|(_, child)| *child));
                let position = key.map_or(0, |key| {
                    separators.partition_point(|(separator, _)| separator.as_slice() <= key)
                });
                expected_level = expected_level
                    .checked_sub(1)
                    .ok_or_else(|| corrupt(page_id, "internal node below leaf level"))?;
                page_id = children[position];
                frames.push(CursorFrame {
                    children,
                    position,
                    child_level: expected_level,
                });
            }
        }
    }
}

fn advance_to_next_leaf(
    storage: &GroupCommitHandle,
    cache: Option<&NodeCache>,
    frames: &mut Vec<CursorFrame>,
    visited: &mut HashSet<PageId>,
) -> Result<Option<PageId>> {
    while let Some(frame) = frames.last_mut() {
        if frame.position + 1 < frame.children.len() {
            frame.position += 1;
            let page_id = frame.children[frame.position];
            let child_level = frame.child_level;
            return descend_to_leaf(storage, cache, page_id, child_level, None, frames, visited)
                .map(|(page_id, _)| Some(page_id));
        }
        frames.pop();
    }
    Ok(None)
}

fn read_node(
    storage: &GroupCommitHandle,
    cache: Option<&NodeCache>,
    page_id: PageId,
) -> Result<Arc<Node>> {
    if let Some(node) = cache.and_then(|cache| cache.get(page_id)) {
        return Ok(node);
    }
    let page = storage
        .read_page(page_id)?
        .ok_or_else(|| corrupt(page_id, "referenced page does not exist"))?;
    let node = Arc::new(Node::decode(page_id, page.payload())?);
    if let Some(cache) = cache {
        cache.insert(page_id, &node);
    }
    Ok(node)
}

fn corrupt(page_id: PageId, reason: impl Into<String>) -> IndexError {
    IndexError::CorruptNode {
        page_id,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantadb_storage::{DurableStore, GroupCommitOptions, GroupCommitter, StoreOptions};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn open(path: &std::path::Path) -> (GroupCommitter, GroupCommitHandle) {
        let store = DurableStore::open(path, StoreOptions::default()).expect("open store");
        let committer =
            GroupCommitter::start(store, GroupCommitOptions::default()).expect("committer");
        let handle = committer.handle();
        (committer, handle)
    }

    fn entries(count: u64) -> Vec<IndexEntry> {
        (0..count)
            .map(|number| IndexEntry {
                key: format!("key:{number:08}").into_bytes(),
                value: PageId(number * 3 + 1),
            })
            .collect()
    }

    #[test]
    fn empty_tree_has_no_root_and_duplicate_keys_are_rejected() {
        let directory = tempdir().expect("tempdir");
        let (committer, handle) = open(directory.path());
        assert_eq!(
            BPlusTree::build(&handle, Vec::<IndexEntry>::new()).expect("empty build"),
            None
        );
        assert!(matches!(
            BPlusTree::build(
                &handle,
                vec![
                    IndexEntry {
                        key: b"a".to_vec(),
                        value: PageId(1),
                    },
                    IndexEntry {
                        key: b"a".to_vec(),
                        value: PageId(2),
                    },
                ],
            ),
            Err(IndexError::KeysNotStrictlyIncreasing)
        ));
        committer.shutdown().expect("shutdown");
    }

    #[test]
    fn split_tree_supports_point_and_bounded_range_reads() {
        let directory = tempdir().expect("tempdir");
        let (committer, handle) = open(directory.path());
        let source = entries(5_000);
        let root = BPlusTree::build(&handle, source.clone())
            .expect("build")
            .expect("root");
        assert!(root.height >= 2, "{root:?}");
        assert_eq!(root.entries, 5_000);

        for position in [0_usize, 1, 400, 4_999] {
            assert_eq!(
                BPlusTree::get(&handle, None, root, &source[position].key).expect("get"),
                Some(source[position].value)
            );
        }
        assert_eq!(
            BPlusTree::get(&handle, None, root, b"missing").expect("missing"),
            None
        );

        let range = BPlusTree::range(
            &handle,
            None,
            root,
            Some(&source[123].key),
            Some(&source[140].key),
            10,
        )
        .expect("range");
        assert_eq!(range, source[123..133]);
        committer.shutdown().expect("shutdown");
    }

    #[test]
    fn durable_generation_survives_storage_restart() {
        let directory = tempdir().expect("tempdir");
        let source = entries(1_000);
        let root;
        {
            let (committer, handle) = open(directory.path());
            root = BPlusTree::build(&handle, source.clone())
                .expect("build")
                .expect("root");
            committer.shutdown().expect("shutdown");
        }
        {
            let (committer, handle) = open(directory.path());
            assert_eq!(
                BPlusTree::range(&handle, None, root, None, None, usize::MAX).expect("scan"),
                source
            );
            committer.shutdown().expect("shutdown");
        }
    }

    #[test]
    fn randomized_model_matches_point_and_range_reads() {
        let directory = tempdir().expect("tempdir");
        let (committer, handle) = open(directory.path());
        let mut random = 0x8f3f_73b5_cf1c_9ade_u64;
        let mut source = Vec::new();
        for number in 0..2_000_u64 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            source.push(IndexEntry {
                key: format!("{random:016x}:{number:08}").into_bytes(),
                value: PageId(number),
            });
        }
        source.sort_by(|left, right| left.key.cmp(&right.key));
        let root = BPlusTree::build(&handle, source.clone())
            .expect("build")
            .expect("root");

        for position in (0..source.len()).step_by(37) {
            assert_eq!(
                BPlusTree::get(&handle, None, root, &source[position].key).expect("get"),
                Some(source[position].value)
            );
        }
        for start in (0..source.len() - 20).step_by(113) {
            let actual = BPlusTree::range(
                &handle,
                None,
                root,
                Some(&source[start].key),
                Some(&source[start + 20].key),
                usize::MAX,
            )
            .expect("range");
            assert_eq!(actual, source[start..start + 20]);
        }
        committer.shutdown().expect("shutdown");
    }

    #[test]
    fn copy_on_write_edits_replace_only_reachable_paths() {
        let directory = tempdir().expect("tempdir");
        let (committer, handle) = open(directory.path());
        let source = entries(5_000);
        let root = BPlusTree::build(&handle, source.clone())
            .expect("build")
            .expect("root");
        let plan = BPlusTree::edit_plan(
            &handle,
            None,
            Some(root),
            [
                IndexMutation::Upsert(IndexEntry {
                    key: source[1_234].key.clone(),
                    value: PageId(900_001),
                }),
                IndexMutation::Delete(source[2_345].key.clone()),
                IndexMutation::Upsert(IndexEntry {
                    key: b"key:00001234:after".to_vec(),
                    value: PageId(900_002),
                }),
            ],
        )
        .expect("edit plan");
        let edited_root = plan.root().expect("edited root");
        assert!(plan.writes.len() < 20, "writes: {}", plan.writes.len());
        handle.commit(plan.into_writes()).expect("commit edit");

        assert_eq!(
            BPlusTree::get(&handle, None, edited_root, &source[1_234].key).expect("updated get"),
            Some(PageId(900_001))
        );
        assert_eq!(
            BPlusTree::get(&handle, None, edited_root, &source[2_345].key).expect("deleted get"),
            None
        );
        assert_eq!(
            BPlusTree::get(&handle, None, edited_root, b"key:00001234:after").expect("inserted get"),
            Some(PageId(900_002))
        );

        let range = BPlusTree::range(
            &handle,
            None,
            edited_root,
            Some(b"key:00001233"),
            Some(b"key:00001236"),
            usize::MAX,
        )
        .expect("edited range");
        assert_eq!(
            range,
            vec![
                source[1_233].clone(),
                IndexEntry {
                    key: source[1_234].key.clone(),
                    value: PageId(900_001),
                },
                IndexEntry {
                    key: b"key:00001234:after".to_vec(),
                    value: PageId(900_002),
                },
                source[1_235].clone(),
            ]
        );
        committer.shutdown().expect("shutdown");
    }

    #[test]
    fn randomized_copy_on_write_edits_match_ordered_map() {
        let directory = tempdir().expect("tempdir");
        let (committer, handle) = open(directory.path());
        let source = entries(1_000);
        let root = BPlusTree::build(&handle, source.clone())
            .expect("build")
            .expect("root");
        let mut model = source
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<BTreeMap<_, _>>();
        let mut mutations = Vec::new();
        let mut random = 0xa076_1d64_78bd_642f_u64;

        for step in 0..2_000_u64 {
            random = random
                .wrapping_mul(2_862_933_555_777_941_757)
                .wrapping_add(3_037_000_493);
            let key = format!("key:{:08}", random % 1_500).into_bytes();
            if random & 3 == 0 {
                model.remove(&key);
                mutations.push(IndexMutation::Delete(key));
            } else {
                let value = PageId(100_000 + step);
                model.insert(key.clone(), value);
                mutations.push(IndexMutation::Upsert(IndexEntry { key, value }));
            }
        }

        let plan = BPlusTree::edit_plan(&handle, None, Some(root), mutations).expect("edit plan");
        let edited_root = plan.root().expect("edited root");
        handle.commit(plan.into_writes()).expect("commit edits");
        let actual = BPlusTree::range(&handle, None, edited_root, None, None, usize::MAX)
            .expect("scan edited tree")
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, model);
        assert_eq!(edited_root.entries, model.len() as u64);
        committer.shutdown().expect("shutdown");
    }
}
