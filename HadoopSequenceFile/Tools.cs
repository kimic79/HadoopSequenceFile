using System;
using System.IO;
using zlib;

namespace HadoopSequenceFile
{
    public class Tools
    {
        public static int ReadInt(Stream stream)
        {
            var buf = new byte[4];
            buf[3] = (byte)stream.ReadByte();
            buf[2] = (byte)stream.ReadByte();
            buf[1] = (byte)stream.ReadByte();
            buf[0] = (byte)stream.ReadByte();
            return BitConverter.ToInt32(buf, 0);
        }

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

        public static void WriteVInt(Stream stream, int i)
        {
            WriteVLong(stream, i);
        }


        public static void WriteVLong(Stream stream, long i)
        {
            if (i >= -112 && i <= 127)
            {
                stream.WriteByte((byte)i);
                return;
            }

            int len = -112;
            if (i < 0)
            {
                i ^= -1L; // take one's complement'
                len = -120;
            }

            long tmp = i;
            while (tmp != 0)
            {
                tmp = tmp >> 8;
                len--;
            }

            stream.WriteByte((byte)len);

            len = (len < -120) ? -(len + 120) : -(len + 112);

            for (int idx = len; idx != 0; idx--)
            {
                int shiftbits = (idx - 1) * 8;
                long mask = 0xFFL << shiftbits;
                stream.WriteByte((byte)((i & mask) >> shiftbits));
            }
        }

        public static void WriteInt(Stream stream, int i)
        {
            byte[] intBytes = BitConverter.GetBytes(i);
            Array.Reverse(intBytes);
            stream.Write(intBytes, 0, 4);

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

        public static void Compress(byte[] inData, out byte[] outData)
        {
            using (MemoryStream outMemoryStream = new MemoryStream())
            using (ZOutputStream outZStream = new ZOutputStream(outMemoryStream, zlibConst.Z_DEFAULT_COMPRESSION))
            using (Stream inMemoryStream = new MemoryStream(inData))
            {
                CopyStream(inMemoryStream, outZStream);
                outZStream.finish();
                outData = outMemoryStream.ToArray();
            }
        }

        public static void CopyStream(Stream input, Stream output)
        {
            byte[] buffer = new byte[2000];
            int len;
            while ((len = input.Read(buffer, 0, 2000)) > 0)
            {
                output.Write(buffer, 0, len);
            }
            output.Flush();
        }

    }
}
