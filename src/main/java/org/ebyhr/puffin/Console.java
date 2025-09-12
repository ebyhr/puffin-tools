/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.puffin;

import org.apache.iceberg.Files;
import org.apache.iceberg.deletes.RoaringPositionBitmaps;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.util.Pair;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.iceberg.puffin.StandardBlobTypes.DV_V1;

@Command(
        name = "puffin",
        header = "Puffin command line interface",
        synopsisHeading = "%nUSAGE:%n%n",
        optionListHeading = "%nOPTIONS:%n",
        usageHelpAutoWidth = true)
public class Console
        implements Callable<Integer>
{
    @Option(names = "--path", paramLabel = "<path>", description = "File path to Puffin")
    public String path;

    private static final int BITMAP_DATA_OFFSET = 4;
    private static final int MAGIC_NUMBER = 1681511377;

    @Override
    public Integer call()
    {
        return run() ? 0 : 1;
    }

    public boolean run()
    {
        try {
            InputFile inputFile = Files.localInput(path);
            try (PuffinReader reader = Puffin.read(inputFile).build()) {
                // metadata
                FileMetadata metadata = reader.fileMetadata();
                for (BlobMetadata blobMetadata : metadata.blobs()) {
                    System.out.println("type: " + blobMetadata.type());
                    System.out.println("inputFields: " + blobMetadata.inputFields());
                    System.out.println("snapshotId: " + blobMetadata.snapshotId());
                    System.out.println("sequenceNumber: " + blobMetadata.sequenceNumber());
                    System.out.println("offset: " + blobMetadata.offset());
                    System.out.println("length: " + blobMetadata.length());
                    System.out.println("compressionCodec: " + blobMetadata.compressionCodec());
                    System.out.println("properties: " + blobMetadata.properties());
                    System.out.println();
                }
                System.out.println("properties: " + metadata.properties());
                // blobs
                for (Pair<BlobMetadata, ByteBuffer> read : reader.readAll(metadata.blobs())) {
                    BlobMetadata blobMetadata = read.first();
                    System.out.println("type: " + blobMetadata.type());
                    if (blobMetadata.type().equals(DV_V1)) {
                        ByteBuffer buffer = read.second();
                        int bitmapDataLength = buffer.getInt();
                        RoaringPositionBitmaps bitmap = deserializeBitmap(buffer.array(), bitmapDataLength);
                        List<Long> deletedRows = new ArrayList<>();
                        bitmap.forEach(deletedRows::add);
                        System.out.println("deletedRows: " + deletedRows);
                    }
                }
            }
        }
        catch (RuntimeException | IOException e) {
            return false;
        }
        return true;
    }

    private static RoaringPositionBitmaps deserializeBitmap(byte[] bytes, int bitmapDataLength)
    {
        ByteBuffer bitmapData = pointToBitmapData(bytes, bitmapDataLength);
        int magicNumber = bitmapData.getInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new RuntimeException("Invalid magic number: %s, expected %s".formatted(magicNumber, MAGIC_NUMBER));
        }
        return RoaringPositionBitmaps.deserialize(bitmapData);
    }

    private static ByteBuffer pointToBitmapData(byte[] bytes, int bitmapDataLength)
    {
        ByteBuffer bitmapData = ByteBuffer.wrap(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        bitmapData.order(ByteOrder.LITTLE_ENDIAN);
        return bitmapData;
    }
}
