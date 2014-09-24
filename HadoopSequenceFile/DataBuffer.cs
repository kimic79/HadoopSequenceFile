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
        private byte[] buffer;                
        private int pos;

        public int Position { get { return pos; } }                

        public DataBuffer()
        {
            this.buffer = new byte[0];
        }

        public DataBuffer(int size)
        {
            this.buffer = new byte[size];
        }
        public bool EOF { get { return pos >= buffer.Length; } }

        public void Clear()
        {
            Set(new byte[0], 0, 0);
        }
        public void Set(byte[] input, int start, int length)
        {
            this.buffer = input;                        
            this.pos = start;
        }

        private byte getByte()
        {
            var res = buffer[pos];
            pos++;
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
            Array.Copy(buffer, pos, buf, 0, length);
            pos += length;
            return buf;
        }
    }
}
