using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadoopSequenceFile
{
    public class Tools
    {
      

        public static int ReadVInt(Stream stream)
        {
            return (int)ReadVLong(stream);
        }

        public static long ReadVLong(Stream stream)
        {
            sbyte firstByte = (sbyte)stream.ReadByte();
            int len = DecodeVIntSize(firstByte);
            if (len == 1)
            {
                return firstByte;
            }
            long i = 0;
            for (int idx = 0; idx < len - 1; idx++)
            {
                byte b = (byte)stream.ReadByte();
                i = i << 8;
                i = i | (b & 0xFFL);
            }
            return (IsNegativeVInt(firstByte) ? (i ^ -1L) : i);
        }

        public static bool IsNegativeVInt(int value)
        {
            return value < -120 || (value >= -112 && value < 0);
        }

        public static int DecodeVIntSize(int value)
        {
            if (value >= -112)
            {
                return 1;
            }
            else if (value < -120)
            {
                return -119 - value;
            }
            return -111 - value;
        }
    }
}
