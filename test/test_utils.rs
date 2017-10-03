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

use exonum_rocksdb::{DB, Options};

use exonum_rocksdb::utils::get_cf_names;
use std::path::Path;

#[test]
fn test_get_cf_names() {
    let path = Path::new("/tmp/rocksdb_test_utils");
    let mut opts = Options::default();

    {
        opts.create_if_missing(true);
        let mut db = DB::open(&opts, path).unwrap();
        let cf1 = db.create_cf("cf1", &opts).unwrap();
        let cf2 = db.create_cf("cf2", &opts).unwrap();
        let _ = db.put_cf(cf1, b"a", b"a");
        let _ = db.put_cf(cf2, b"a", b"a");
    }

    let cf_names = get_cf_names(path).unwrap();
    assert_eq!(cf_names, vec!["default", "cf1", "cf2"]);

    {
        let cf_names_str: Vec<&str> = cf_names.iter().map(|cf| cf.as_ref()).collect();
        let db = DB::open_cf(&opts, path, cf_names_str.as_ref()).unwrap();
        let cf1 = db.cf_handle("cf1").unwrap();
        let cf2 = db.cf_handle("cf2").unwrap();

        assert!(db.get_cf(cf1, b"a").is_ok());
        assert!(db.get_cf(cf2, b"a").is_ok());
    }

    if let Err(e) = DB::destroy(&opts, path) {
        println!("couldn't destroy database: {}", e);
    }
}
