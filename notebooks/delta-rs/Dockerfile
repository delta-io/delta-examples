FROM rust:1.72

WORKDIR /usr/src/delta-rs

RUN set -ex \
    && deps=" \
      python3 \
      python3-dev \
      python3-pip \
    " \
    && apt-get update -y && apt-get install -y $deps --no-install-recommends

RUN pip3 install jupyter --break-system-packages

RUN rustup component add rust-src
RUN cargo install --locked evcxr_jupyter
RUN evcxr_jupyter --install

CMD ["jupyter", "notebook", "--ip", "0.0.0.0", "--no-browser", "--allow-root"]