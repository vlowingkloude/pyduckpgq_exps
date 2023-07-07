FROM fedora

RUN dnf up -y

RUN dnf install -y cmake gcc g++ make python3 python3-pip git python3-devel vim

RUN pip3 install pandas psutil

RUN dnf install -y ninja-build

COPY duckpgq-extension /duckdb

RUN dnf install -y zstd

#RUN pip install cmakelang

#RUN cd /duckdb/tools/pythonpkg && python3 setup.py install # pytest
#RUN cd /duckdb/ &&  EXTENSION_STATIC_BUILD=1 BUILD_JEMALLOC=1 BUILD_SQLPGQ=1 BUILD_PYTHON=1 GEN=ninja make GEN=ninja
RUN cd /duckdb && BUILD_PYTHON=1 make GEN=ninja release_python
COPY duckpgq-experiments /duckexp
RUN ./duckexp/scripts/get-small-data.sh
#COPY bench_framework.py /duckexp/data/
#COPY data_loaders.py /duckexp/data/
#COPY genquery.py /duckexp/data
WORKDIR /duckexp/data/

#RUN pip3 install pyarrow polars
#COPY duckpgq.py /
RUN pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
#RUN pip install torch_geometric
#RUN pip install pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv -f https://data.pyg.org/whl/torch-2.0.0+cpu.html
RUN pip install  dgl -f https://data.dgl.ai/wheels/repo.html
RUN pip install  dglgo -f https://data.dgl.ai/wheels-test/repo.html
WORKDIR /duckexp/data/
COPY pyduckpgq/duckpgq.py /duckexp/data/duckpgq.py
RUN pip install polars pyarrow
RUN pip install torch_geometric
RUN pip install pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv -f https://data.pyg.org/whl/torch-2.0.0+cpu.html
COPY pyduckpgq/decorators.py /duckexp/data/
COPY bench_framework.py /duckexp/data/
COPY data_loaders.py /duckexp/data/
COPY genquery.py /duckexp/data
