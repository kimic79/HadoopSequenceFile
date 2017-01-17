using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadoopSequenceFile
{
    public class DataBuffer
    {        
        private MemoryStream stream = new MemoryStream();

        public long Position { get { return stream.Position; } }                
        public long Length { get { return stream.Length; } }
        public DataBuffer()
        {
            this.stream = new MemoryStream();
        }

        public DataBuffer(int size)
        {
            this.stream = new MemoryStream(size);
        }
        public bool EOF
        {
            get { return Position >= stream.Length; }
        }

        public void Clear()
        {
            Set(new byte[0], 0, 0);
        }
        public void Set(byte[] input, int start, int length)
        {
            this.stream = new MemoryStream(input);            
        }

        private byte getByte()
        {
            var res = Convert.ToByte(stream.ReadByte());
            return res;
        }
        internal int GetVInt()
        {
            return (int)GetVLong();
        }
        internal long GetVLong()
        {
            sbyte firstByte = (sbyte) getByte();
            int len = Tools.DecodeVIntSize(firstByte);
            if (len == 1)
            {
                return firstByte;
            }
            long i = 0;
            for (int idx = 0; idx < len - 1; idx++)
            {
                byte b = getByte();
                i = i << 8;
                i = i | (b & 0xFFL);
            }
            return (Tools.IsNegativeVInt(firstByte) ? (i ^ -1L) : i);
        }

        internal byte[] GetBytes(int length)
        {
            byte[] buf = new byte[length];
            int rcnt = stream.Read(buf, 0, length);            
            return buf;
        }

        public void AddVInt(int l)
        {
            Tools.WriteVInt(stream, l);
        }

        public void AddVLong(long l)
        {
            Tools.WriteVLong(stream, l);
        }

        public int AddData(byte[] data, bool compress = false)
        {
            if (compress)
            {
                byte[] content;
                Tools.Compress(data, out content);
                stream.Write(content, 0, content.Length);
                return content.Length;
            } else
            {
                stream.Write(data, 0, data.Length);
                return data.Length;
            }
        }

        public void Write(Stream outputStream)
        {            
            stream.CopyTo(outputStream);            
        }

        public byte[] ToArray()
        {
            stream.Seek(0, 0);
            return stream.ToArray();
        }       
    }
}
