# puffin-tools

# Building puffin-tools
puffin-tools is a standard Maven project. Simply run the following command from the project root directory:
```
./mvnw clean install -DskipTests
```

# Running puffin-tools
Run puffin-tools with `path` option:
```
target/puffin-tools-*-executable.jar --path path-to-puffin-file
```

It prints Puffin file information:
```
type: apache-datasketches-theta-v1
inputFields: [1]
snapshotId: 3422191382050543010
sequenceNumber: 1
offset: 4
length: 69
compressionCodec: zstd
properties: {ndv=5}

type: apache-datasketches-theta-v1
inputFields: [2]
snapshotId: 3422191382050543010
sequenceNumber: 1
offset: 73
length: 69
compressionCodec: zstd
properties: {ndv=5}

type: apache-datasketches-theta-v1
inputFields: [3]
snapshotId: 3422191382050543010
sequenceNumber: 1
offset: 142
length: 69
compressionCodec: zstd
properties: {ndv=5}

properties: {created-by=Trino version testversion}
```
