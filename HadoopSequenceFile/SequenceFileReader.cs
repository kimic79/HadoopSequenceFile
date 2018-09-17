using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using zlib;

namespace HadoopSequenceFile
{
    public class SequenceFileReader : IDisposable
    {
        private const string HadoopCompressGzipCodec = "org.apache.hadoop.io.compress.GzipCodec";
        private const string HadoopCompressDefaultCodec = "org.apache.hadoop.io.compress.DefaultCodec";

        private Stream input;
        private KeyValuePair<byte[], byte[]> _dataKV = new KeyValuePair<byte[], byte[]>();
        private BinaryReader reader;
        public KeyValuePair<byte[], byte[]> Data { get { return _dataKV; } }
        private static byte BLOCK_COMPRESS_VERSION = (byte)4;
        private static byte CUSTOM_COMPRESS_VERSION = (byte)5;
        private static byte VERSION_WITH_METADATA = (byte)6;
        private static int SYNC_HASH_SIZE = 16;
        private static int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash
        private static int SYNC_ESCAPE = -1;
        public static int SYNC_INTERVAL = 100 * SYNC_SIZE;
        private byte[] metadata = null;
        private byte[] sync = new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
        private bool compressed;
        private bool zlibCompressed;
        private bool blockCompressed;
        private byte version;
        private int noBufferedRecords = 0;
        private int keyPosition = 0;
        private DataBuffer keyLenBuffer = null;
        private DataBuffer keyBuffer = null;
        private DataBuffer valLenBuffer = null;
        private DataBuffer valBuffer = null;
        public bool IsCompressed { get { return compressed; } }
        public bool IsBlockCompressed { get { return blockCompressed; } }
        public byte Version { get { return version; } }
        private string keyClassName;
        private string valueClassName;
        private byte[] key = null;
        private byte[] value = null;
        public byte[] Key { get { return key; } }
        public byte[] Value { get { return value; } }
        private bool blockIsReady = false; // block is ready to process to key-values
        private bool syncSeen = false;

        public SequenceFileReader(Stream stream)
        {
            this.input = stream;
            this.reader = new BinaryReader(stream);
            initHeader();
        }

        private void initHeader()
        {
            var hdr = this.reader.ReadChars(3);
            var shdr = new string(hdr);
            if (!shdr.Equals("SEQ"))
                throw new Exception("Invalid header " + shdr);
            this.version = this.reader.ReadByte();
            keyClassName = readString();
            valueClassName = readString();
            if (version > 2)
            {                          // if version > 2
                this.compressed = this.reader.ReadBoolean();       // is compressed?
            }
            else
            {
                compressed = false;
            }
            valBuffer = new DataBuffer();
            if (version >= BLOCK_COMPRESS_VERSION)
            {    // if version >= 4
                this.blockCompressed = this.reader.ReadBoolean();  // is block-compressed?
                keyLenBuffer = new DataBuffer();
                keyBuffer = new DataBuffer();
                valLenBuffer = new DataBuffer();
            }
            else
            {
                blockCompressed = false;
            }
            // if version >= 5
            // setup the compression codec
            if (compressed)
            {
                if (version >= CUSTOM_COMPRESS_VERSION)
                {
                    string codecClassname = readString();
                    if (codecClassname != HadoopCompressDefaultCodec && codecClassname != HadoopCompressGzipCodec)
                        throw new Exception("Unknown codec " + codecClassname);
                    zlibCompressed = codecClassname == HadoopCompressDefaultCodec;
                    blockCompressed = codecClassname == HadoopCompressGzipCodec;
                }

            }
            {
                if (version >= VERSION_WITH_METADATA)
                {    // if version >= 6
                    this.metadata = reader.ReadBytes(4);
                }
                if (version > 1)
                {
                    sync = reader.ReadBytes(SYNC_HASH_SIZE);
                }
            }
        }

        private void readCompressedBuffer(DataBuffer buffer)
        {
            var buf = readCompressedBuffer();
            buffer.Set(buf, 0, buf.Length);
        }

        private string readString()
        {
            int len = Tools.ReadVInt(input);
            byte[] strbuf = new byte[len];
            input.Read(strbuf, 0, len);
            return Encoding.UTF8.GetString(strbuf);
        }

        private bool readCompressedBlock()
        {
            // Reset internal states
            noBufferedRecords = 0;
            keyPosition = 0;
            //Process sync
            if (input.Position == input.Length)
                return false; //eof
            if (version > 1 && sync != null)
            {
                reader.ReadInt32(); // skip 4 bytes
                syncCheck = reader.ReadBytes(SYNC_HASH_SIZE);
                if (!sync.SequenceEqual(syncCheck))
                    throw new IOException("File is corrupt!");
                if (input.Position == input.Length)
                    return false; //eof
            }
            // Read number of records in this block
            noBufferedRecords = Tools.ReadVInt(input);

            // Read key lengths and keys
            readCompressedBuffer(keyLenBuffer);
            readCompressedBuffer(keyBuffer);
            readCompressedBuffer(valLenBuffer);
            readCompressedBuffer(valBuffer);
            blockIsReady = true;
            return true;
        }


        private int readRecordLength()
        {
            if (input.Position >= input.Length)
                return -1;

            int length = Tools.ReadInt(input);
            if (version > 1 && sync != null && length == SYNC_ESCAPE)
            {
                // process a sync entry
                syncCheck = reader.ReadBytes(SYNC_HASH_SIZE);
                if (!sync.SequenceEqual(syncCheck))
                    throw new IOException("File is corrupt!");
                syncSeen = true;
                if (input.Position >= input.Length)
                    return -1;
                length = Tools.ReadInt(input); // re-read length
            }
            else
            {
                syncSeen = false;
            }
            return length;
        }

        private bool readCompressedRecord()
        {
            if (input.Position == input.Length)
                return false; //eof
            // Read record length
            var recordLength = readRecordLength();
            // Read key length
            var keyLength = Tools.ReadInt(input);
            if (recordLength < 0 || keyLength < 0)
                throw new Exception("Broken data length");
            // Read key
            this.key = new byte[keyLength];
            input.Read(key, 0, keyLength);
            // Read value
            this.value = readCompressedBuffer(recordLength - keyLength);
            return true;
        }

        private byte[] readCompressedBuffer()
        {
            var zLength = Tools.ReadVInt(input);
            return readCompressedBuffer(zLength);
        }

        private byte[] readCompressedBuffer(int zLength)
        {
            byte[] compressed = new byte[zLength];
            input.Read(compressed, 0, zLength);
            MemoryStream zinput = new MemoryStream(compressed);
            MemoryStream uncompressed = new MemoryStream(zLength);

            if (zlibCompressed)
            {
                using (var zstream = new zlib.ZInputStream(zinput))
                {
                    byte[] buf = new byte[1024];
                    int len = 0;
                    while ((len = zstream.read(buf, 0, buf.Length)) > 0)
                    {
                        uncompressed.Write(buf, 0, len);
                    }

                    uncompressed.Flush();
                }
            }
            else
            {
                using (var gZipStream = new GZipStream(zinput, CompressionMode.Decompress))
                {
                    byte[] buf = new byte[1024];
                    int len = 0;
                    while ((len = gZipStream.Read(buf, 0, buf.Length)) > 0)
                    {
                        uncompressed.Write(buf, 0, len);
                    }

                    uncompressed.Flush();
                }
            }
            return uncompressed.ToArray();
        }

        public bool Read()
        {
            if (IsBlockCompressed)
            {
                return ReadBlockKeyValue();
            }
            else if (IsCompressed)
            {
                return readCompressedRecord();
            }
            return readRecord();
        }

        private bool readRecord()
        {
            if (input.Position == input.Length)
                return false; //eof
            // Read record length
            var recordLength = readRecordLength();
            // Read key length
            var keyLength = Tools.ReadInt(input);
            if (recordLength < 0 || keyLength < 0)
                throw new Exception("Broken data length");
            // Read key
            this.key = new byte[keyLength];
            input.Read(key, 0, keyLength);
            // Read value
            if (valueClassName == "org.apache.hadoop.io.Text")
            {
                var res = readString();
                this.value = Encoding.UTF8.GetBytes(res);
            }
            else
            {
                byte[] content = new byte[recordLength - keyLength];
                input.Read(content, 0, recordLength - keyLength);
                this.value = content;
            }
            return true;
        }

        public void Dispose()
        {
            this.reader.Dispose();
        }

        public bool ReadBlockKeyValue()
        {
            if (IsBlockCompressed)
            {
                try
                {
                    // initial load
                    if (!blockIsReady)
                        if (!readCompressedBlock())
                            return false;  // no more records ready

                    if (keyLenBuffer.EOF || keyBuffer.EOF || valLenBuffer.EOF || (valBuffer.EOF && zlibCompressed))
                        throw new Exception("Source block has invalid number of assigned key or value buffers");
                    int klen = keyLenBuffer.GetVInt();
                    this.key = keyBuffer.GetBytes(klen);
                    int vlen = valLenBuffer.GetVInt();
                    this.value = valBuffer.GetBytes(vlen);
                    keyPosition++;

                    if (keyPosition == noBufferedRecords)
                        blockIsReady = false;         // end of block
                    return true;
                }
                catch (Exception ex)
                {
                    throw new Exception(string.Format("Unable to get element {0}: {1}", keyLenBuffer.Position, ex.Message), ex);
                }
            }
            return false;
        }
    }
}
