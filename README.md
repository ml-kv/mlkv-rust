# MLKV: Efficiently Scaling up Large Embedding Model Training with Disk-based Key-Value Storage
MLKV is a research storage framework that tackles scalability issues of embedding model training, including memory limitations, data staleness, and cache inefficiencies.
MLKV augments key-value storage to provide easy-to-use and non-intrusive interfaces for various machine-learning tasks.

## Applications
* [MLKV-DLRM](https://github.com/ml-kv/mlkv-dlrm): deep learning-based recommendations
* [MLKV-GNN](https://github.com/ml-kv/mlkv-gnn): graph neural networks
* [MLKV-KG](https://github.com/ml-kv/mlkv-kg): knowledge graphs
* [MLKV-PYTHON](https://github.com/ml-kv/mlkv-gnn/blob/dgl-mlkv/python/dgl/storages/mlkv.py): python wrapper

## Build
Ubuntu (20.04 LTS x86/64) packages
```
sudo apt-get update
sudo apt-get install make cmake clang
sudo apt-get install uuid-dev libaio-dev libtbb-dev
```
[Rust](https://www.rust-lang.org/tools/install)
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustc --version
rustup self uninstall
```

## Test
```
git clone -b main git@github.com:ml-kv/mlkv-rust.git rust-mlkv
cargo run --example single_thread
cargo run --example multi_threads
cargo run --example checkpoint_recover
```
