using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace HadoopSequenceFile
{
    public abstract class SequenceFileWriter : IDisposable
    {
        private Stream output;
        private KeyValuePair<byte[], byte[]> _dataKV = new KeyValuePair<byte[], byte[]>();
        protected BinaryWriter writer;
        public KeyValuePair<byte[], byte[]> Data { get { return _dataKV; } }
        private static byte BLOCK_COMPRESS_VERSION = (byte)4;
        private static byte CUSTOM_COMPRESS_VERSION = (byte)5;
        private static byte VERSION_WITH_METADATA = (byte)6;
        private static int SYNC_HASH_SIZE = 16;
        private static int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash
        private static int SYNC_ESCAPE = -1;
        public static int SYNC_INTERVAL = 100 * SYNC_SIZE;
        private long lastSyncPos = 0;
        private byte[] metadata = new byte[4];
        private byte[] sync = new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
        private byte version;
        private int noBufferedRecords = 0;
        private int keyPosition = 0;
        protected abstract bool isCompressed { get; }
        protected abstract bool isBlockCompressed {get;}
        public byte Version { get { return version; } }
        protected string keyClassName = "org.apache.hadoop.io.BytesWritable";
        protected string valueClassName = "org.apache.hadoop.io.BytesWritable";
        private byte[] key = null;
        private byte[] value = null;
        public byte[] Key { get { return key; } }
        public byte[] Value { get { return value; } }
        private bool blockIsReady = false; // block is ready to process to key-values
        private bool syncSeen = false;
        private CompressionFileType fileType;
        private string codecClassname = "org.apache.hadoop.io.compress.DefaultCodec";
        private int compressionBlockSize = 1000000;
        public SequenceFileWriter(Stream stream, CompressionFileType fileType, string keyClassName =  "org.apache.hadoop.io.BytesWritable", string valueClassName = "org.apache.hadoop.io.BytesWritable")
        {
            this.output = stream;
            this.fileType = fileType;
            this.keyClassName = keyClassName;
            this.valueClassName = valueClassName;
            this.writer = new BinaryWriter(stream);
            initHeader();
        }

        protected abstract void append(byte[] key, byte[] value);

        public void Append(byte[] key, byte[] value)
        {
            append(key, value);
        }

        private void initHeader()
        {
            version = 6;
            var r = new Random(DateTime.Now.Millisecond);
            for (int i = 0; i < SYNC_HASH_SIZE; i++)
            {
                sync[i] = (byte)r.Next(0, 255);
            }
            this.writer.Write(Encoding.ASCII.GetBytes("SEQ"));

            writer.Write(VERSION_WITH_METADATA);
            writeString(keyClassName);
            writeString(valueClassName);
            writer.Write(isCompressed);
            writer.Write(isBlockCompressed);
            if (isCompressed)
            {
                if (version >= CUSTOM_COMPRESS_VERSION)
                {
                    writeString(codecClassname);
                }
            }
            if (version >= VERSION_WITH_METADATA)
            {    // if version >= 6
                writer.Write(metadata, 0, metadata.Length);
            }
            if (version > 1)
            {
                writer.Write(sync, 0, sync.Length);
            }

        }

        protected void checkAndWriteSync()
        {
            if (sync != null && output.Position >= lastSyncPos + SYNC_INTERVAL)
            { // time to emit sync
                writeSync();
            }
        }

        protected void writeSync()
        {
            if (sync != null && lastSyncPos != output.Position)
            {
                var sesc = BitConverter.GetBytes(SYNC_ESCAPE);
                output.Write(sesc, 0, sesc.Length);
                output.Write(sync, 0, sync.Length);
                lastSyncPos = output.Position;
            }
        }

        protected virtual void flush()
        {

        }
        protected void writeVInt(int value)
        {
            Tools.WriteVInt(output, value);
        }

        protected void writeInt(int value)
        {
            Tools.WriteInt(output, value);
        }

        protected void writeVLong(long value)
        {
            Tools.WriteVLong(output, value);
        }
        public void Close()
        {
            if (writer != null)
            {
                writer.Close();
                writer.Dispose();
                writer = null;
            }
        }

        public int writeBytes(byte[] input)
        {
            output.Write(input, 0, input.Length);
            return input.Length;
        }

        protected void writeString(string content)
        {
            var arr = Encoding.UTF8.GetBytes(content);
            Tools.WriteVInt(output, arr.Length);
            output.Write(arr, 0, arr.Length);
        }

        protected byte[] getString(byte[] content)
        {
            using (var ms = new MemoryStream())
            {
                Tools.WriteVInt(ms, content.Length);
                ms.Write(content, 0, content.Length);
                return ms.ToArray();
            }
        }
        public void Flush()
        {
            flush();
            if (writer != null)
            {
                writer.Flush();
            }
        }
        public void Dispose()
        {
            Close();
            if (this.writer != null)
                this.writer.Dispose();
        }
    }
}
