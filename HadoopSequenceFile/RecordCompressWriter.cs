using System.IO;

namespace HadoopSequenceFile
{
    public class RecordCompressWriter : SequenceFileWriter
    {
        protected override bool isBlockCompressed
        {
            get
            {
                return false;
            }
        }

        protected override bool isCompressed
        {
            get
            {
                return true;
            }
        }

        public RecordCompressWriter(Stream stream, string keyClassName = "org.apache.hadoop.io.BytesWritable", string valueClassName = "org.apache.hadoop.io.BytesWritable")
            : base(stream, CompressionFileType.Compressed, keyClassName, valueClassName)
        {

        }

        protected override void append(byte[] key, byte[] value)
        {
            checkAndWriteSync();
            byte[] content;
            Tools.Compress(value, out content);
            writeInt(content.Length + key.Length);
            writeInt(key.Length);
            writeBytes(key);
            writeBytes(content);
        }

    }
}
