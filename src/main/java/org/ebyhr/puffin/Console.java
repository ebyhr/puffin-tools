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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.concurrent.Callable;

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
            }
        }
        catch (RuntimeException | IOException e) {
            return false;
        }
        return true;
    }
}
