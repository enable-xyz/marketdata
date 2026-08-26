FROM scratch
ARG TARGETARCH
COPY dist/enable-market-linux-${TARGETARCH} /enable-market
USER 65532:65532
ENTRYPOINT ["/enable-market"]
CMD ["smoke", "--role", "all"]
