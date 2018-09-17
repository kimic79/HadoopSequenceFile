using System.IO;

namespace HadoopSequenceFile
{
    public class RecordWriter : SequenceFileWriter
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
                return false;
            }
        }
        public RecordWriter(Stream stream, string keyClassName = "org.apache.hadoop.io.BytesWritable", string valueClassName = "org.apache.hadoop.io.BytesWritable")
            : base(stream, CompressionFileType.Compressed, keyClassName, valueClassName)
        {

        }

        protected override void append(byte[] key, byte[] value)
        {
            checkAndWriteSync();
            if (valueClassName == "org.apache.hadoop.io.Text")
                value = getString(value);
            writeInt(key.Length + value.Length);
            writeInt(key.Length);
            writeBytes(key);
            writeBytes(value);
        }
    }
}
