using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadoopSequenceFile
{
    public enum CompressionFileType
    {
        Uncompressed = 1,
        Compressed = 3,
        BlockCompressed = 4,
        CustomCompressVersion = 5,
        VersionWithMetadata = 6
    }
}
