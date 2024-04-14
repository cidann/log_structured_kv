use std::ops::Range;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::{KvStore, KvsEngine};
use rand::{thread_rng, Rng,distributions::Alphanumeric,seq::SliceRandom};
use tempfile::TempDir;

fn generate_string(len_range:Range<usize>)->String{
    let mut rng=thread_rng();
    let len=rng.gen_range(len_range);
    rng
    .sample_iter(&Alphanumeric)
    .take(len)
    .map(char::from)
    .collect()
}

pub fn storage_benchmark(c:&mut Criterion){
    let mut group=c.benchmark_group("storage");
    let set_input=vec![(generate_string(1..10000),generate_string(1..10000));1000];
    let mut get_input=set_input.iter().map(|(k,_)|k.clone()).collect::<Vec<_>>();
    get_input.shuffle(&mut thread_rng());
    
    group.bench_function(
        BenchmarkId::new("sled", 1000),
        |b| b.iter_batched(
            ||{
                let kv=TempDir::new().unwrap();
                let db=sled::open(kv.path()).unwrap();
                db
            },
            |mut storage|{
                for (k,v) in set_input.iter(){
                    storage.set(k.clone(), v.clone()).unwrap();
                }
                for key in get_input.iter(){
                    storage.get(key.clone()).unwrap();
                }
            },
            criterion::BatchSize::PerIteration
        )
    );
    group.bench_function(
        BenchmarkId::new("kv", 1000),
        |b| b.iter_batched(
            ||{
                let tmp_dir=TempDir::new().unwrap();
                let db=KvStore::open(tmp_dir.path()).unwrap();
                (tmp_dir,db)
            },
            |(tmp_dir,mut storage)|{
                for (k,v) in set_input.iter(){
                    storage.set(k.clone(), v.clone()).unwrap();
                }
                for key in get_input.iter(){
                    storage.get(key.clone()).unwrap();
                }
            },
            criterion::BatchSize::PerIteration
        )
    );
}


criterion_group!(benches,storage_benchmark);
criterion_main!(benches);