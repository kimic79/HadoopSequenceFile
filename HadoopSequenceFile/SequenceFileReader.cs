using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadoopSequenceFile
{
    public class SequenceFileReader : IDisposable
    {
        private Stream input;        
        private KeyValuePair<byte[], byte[]> _dataKV = new KeyValuePair<byte[], byte[]>();
        private BinaryReader reader;
        public KeyValuePair<byte[], byte[]> Data { get { return _dataKV; } }
        private static byte BLOCK_COMPRESS_VERSION = (byte)4;
        private static byte CUSTOM_COMPRESS_VERSION = (byte)5;
        private static byte VERSION_WITH_METADATA = (byte)6;        
        private static int SYNC_HASH_SIZE = 16;
        private static int SYNC_SIZE = 4 + SYNC_HASH_SIZE; // escape + hash
        public static int SYNC_INTERVAL = 100 * SYNC_SIZE;
        private byte[] metadata = null;
        private byte[] sync = new byte[SYNC_HASH_SIZE];
        private byte[] syncCheck = new byte[SYNC_HASH_SIZE];        
        private bool decompress;
        private bool blockCompressed;
        private byte version;
        private int noBufferedRecords = 0;        
        private int keyPosition = 0;        
        private DataBuffer keyLenBuffer = null;
        private DataBuffer keyBuffer = null;
        private DataBuffer valLenBuffer = null;
        private DataBuffer valBuffer = null;
        public bool IsCompressed { get { return decompress; } }
        public bool IsBlockCompressed { get { return blockCompressed; } }
        public byte Version { get { return version; } }
        private string keyClassName;
        private string valueClassName;

        private byte[] key = null;
        private byte[] value = null;
        public byte[] Key { get { return key; } }
        public byte[] Value { get { return value; } }

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
                this.decompress = this.reader.ReadBoolean();       // is compressed?
            }
            else
            {
                decompress = false;
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
            if (decompress)
            {
                if (version >= CUSTOM_COMPRESS_VERSION)
                {
                    string codecClassname = readString();
                    if (codecClassname != "org.apache.hadoop.io.compress.DefaultCodec")
                        throw new Exception("Unknown codec " + codecClassname);
                }


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

        private void readBuffer(DataBuffer buffer)
        {
            var buf = readBuffer();
            buffer.Set(buf, 0, buf.Length);
        }

        private string readString()
        {
            int len = Tools.ReadVInt(input);
            byte[] strbuf = new byte[len];
            input.Read(strbuf, 0, len);
            return Encoding.UTF8.GetString(strbuf);
        }

        private bool readBlock()
        {
            // Reset internal states            
            noBufferedRecords = 0;
            keyPosition = 0;
            //Process sync
            if (sync != null)
            {
                if (input.Position == input.Length)
                    return false; //eof
                reader.ReadInt32();
                syncCheck = reader.ReadBytes(SYNC_HASH_SIZE);
                if (Array.Equals(sync, syncCheck))
                    throw new IOException("File is corrupt!");
            }
            // Read number of records in this block
            noBufferedRecords = Tools.ReadVInt(input);

            // Read key lengths and keys
            readBuffer(keyLenBuffer);
            readBuffer(keyBuffer);
            readBuffer(valLenBuffer);
            readBuffer(valBuffer);
            
            return true; // eof??
        }

        private byte[] readBuffer()
        {
            var zLength = Tools.ReadVInt(input);
            byte[] compressed = new byte[zLength];
            input.Read(compressed, 0, zLength);
            MemoryStream zinput = new MemoryStream(compressed);
            MemoryStream uncompressed = new MemoryStream(zLength);
            
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
            return uncompressed.ToArray();
        }

        public bool Read()
        {
            return readBlock();
        }

        public void Dispose()
        {
            this.reader.Dispose();
        }

        public bool ReadKeyValue()
        {
            try
            {
                if (keyPosition == noBufferedRecords) // end of block
                    return false;
                if (keyLenBuffer.EOF || keyBuffer.EOF || valLenBuffer.EOF || valBuffer.EOF)
                    throw new Exception("Source block has invalid number of assigned key or value buffers");
                int klen = keyLenBuffer.GetVInt();
                this.key = keyBuffer.GetBytes(klen);
                int vlen = valLenBuffer.GetVInt();
                this.value = valBuffer.GetBytes(vlen);
                keyPosition++;
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Unable to get element {0}: {1}", keyLenBuffer.Position, ex.Message), ex);
            }
        }
    }
}
