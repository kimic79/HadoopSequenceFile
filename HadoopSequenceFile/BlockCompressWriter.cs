using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadoopSequenceFile
{
    public class BlockCompressWriter : SequenceFileWriter
    {
        private DataBuffer keyLenBuffer = new DataBuffer();
        private DataBuffer keyBuffer = new DataBuffer();
        private DataBuffer valLenBuffer = new DataBuffer();
        private DataBuffer valBuffer = new DataBuffer();

        private int numberOfRecords = 0;
        private int blockRecMax = 100;

        protected override bool isBlockCompressed
        {
            get
            {
                return true;
            }
        }

        protected override bool isCompressed
        {
            get
            {
                return true;
            }
        }

        public BlockCompressWriter(Stream stream, int blockRecMax = 100, string keyClassName = "org.apache.hadoop.io.BytesWritable", string valueClassName = "org.apache.hadoop.io.BytesWritable") 
            : base(stream, CompressionFileType.BlockCompressed, keyClassName, valueClassName)
        {
            this.blockRecMax = blockRecMax;   
        }

        protected override void append(byte[] key, byte[] value)
        {            
            keyLenBuffer.AddVInt(keyBuffer.AddData(key, true));
            valLenBuffer.AddVInt(valBuffer.AddData(value, true));
            numberOfRecords++;
            if (numberOfRecords>= blockRecMax)
            {
                writeBuffers();
            }
        }

        private void writeBuffers()
        {
            if (numberOfRecords == 0)
                return;
            writeVInt(numberOfRecords);
            writeBuffer(keyLenBuffer);
            writeBuffer(keyBuffer);
            writeBuffer(valLenBuffer);
            writeBuffer(valBuffer);
            numberOfRecords = 0;
            writeSync();
        }

        private void writeBuffer(DataBuffer buffer)
        {
            var data = buffer.ToArray();
            writer.Write(data, 0, data.Length);            
            buffer.Clear();            
        }

        protected override void flush()
        {            
            writeBuffers();            
        }
    }
}
