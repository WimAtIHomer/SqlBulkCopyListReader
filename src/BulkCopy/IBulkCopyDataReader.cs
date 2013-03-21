using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace IHomer.Common.BulkCopy
{
    public interface IBulkCopyDataReader: IDataReader
    {
        string DestinationTableName { get; set; }
        List<SqlBulkCopyColumnMapping> ColumnMapping { get; }
    }
}
