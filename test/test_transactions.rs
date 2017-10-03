// Copyright 2017 The Exonum Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use exonum_rocksdb::{TransactionDB, WriteOptions, TransactionOptions, IteratorMode, Options};
use tempdir::TempDir;

#[test]
fn test_transactiondb_creation_and_destroy() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let _ = TransactionDB::open_default(path).unwrap();
    assert!(TransactionDB::destroy(&Options::default(), path).is_ok());
}

#[test]
fn test_transactiondb_commit() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    assert!(txn.put(b"key2", b"value2").is_ok());
    assert!(txn.get(b"key1").unwrap().is_some());
    assert!(txn.delete(b"key2").is_ok());
    assert!(txn.get(b"key2").unwrap().is_none());
    assert!(txn.commit().is_ok());
    assert_eq!(db.get(b"key1").unwrap().unwrap().to_utf8(), Some("value1"));
}

#[test]
fn test_transactiondb_rollback() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    assert!(txn.get(b"key1").unwrap().is_some());
    assert!(txn.rollback().is_ok());
    assert!(db.get(b"key1").unwrap().is_none());
}

#[test]
fn test_transaction_iterator() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    assert!(txn.put(b"key2", b"value2").is_ok());
    assert!(txn.put(b"key3", b"value3").is_ok());
    assert!(txn.put(b"key4", b"value4").is_ok());
    let iter = txn.iterator();
    assert!(iter.valid());
    assert_eq!(iter.count(), 4);
    // assert!(iter.next().is_some());
}

#[test]
fn test_transaction_snapshot() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    assert!(txn.put(b"key2", b"value2").is_ok());
    assert!(txn.commit().is_ok());
    let snapshot = db.snapshot();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key3", b"value3").is_ok());
    let iter = txn.iterator();
    assert!(iter.valid());
    assert_eq!(iter.count(), 3);
    assert!(txn.commit().is_ok());
    let iter = snapshot.iterator(IteratorMode::Start);
    assert!(iter.valid());
    assert_eq!(iter.count(), 2);
}

#[test]
fn test_transaction_savepoint() {
    let temp_dir = TempDir::new("transaction_db").unwrap();
    let path = temp_dir.path();
    let db = TransactionDB::open_default(path).unwrap();
    let w_opts = WriteOptions::default();
    let txn_opts = TransactionOptions::default();
    let txn = db.transaction_begin(&w_opts, &txn_opts);
    assert!(txn.put(b"key1", b"value1").is_ok());
    txn.savepoint();
    assert!(txn.get(b"key1").unwrap().is_some());
    assert!(txn.put(b"key2", b"value2").is_ok());
    assert!(txn.get(b"key2").unwrap().is_some());
    assert!(txn.rollback_to_savepoint().is_ok());
    assert!(txn.get(b"key2").unwrap().is_none());
}
