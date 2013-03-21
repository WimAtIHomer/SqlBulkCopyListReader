using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace IHomer.Common.BulkCopy
{
    /// <summary>
    /// IDataReader implementation for IEnumerable T for use with SqlBulkCopy
    /// </summary>
    /// <typeparam name="T">T can be any object the properties of T have to match the columnnames of the destination Table</typeparam>
    public class BulkCopyListReader<T> : IBulkCopyDataReader
    {
        private IEnumerator<T> _sourceEnumerator;
        private readonly IEnumerable<T> _source;
        private int _affectedRows;
        private static DataTable _schemaTable;
        private static Dictionary<int, Func<T, dynamic>> _properties;
        private static string _destinationTableName = string.Empty;
        private static readonly Object _thisLock = new Object();

        /// <summary>
        ///  Initialize the bulk reader with source and press database id
        /// </summary>
        /// <param name="source">Source enumerator for reading</param>
        /// <param name="connectionString">connectionString of the database connection to use</param>
        /// <param name="destinationTableName">optional tablename if is different that class name</param>
        public BulkCopyListReader(IEnumerable<T> source, string connectionString, string destinationTableName = null)
        {
            if (source == null)
            {
                throw new ArgumentException("source cannot be null");
            }

            _source = source;

            _destinationTableName = string.IsNullOrWhiteSpace(destinationTableName) ? typeof(T).Name : destinationTableName;

            if (_properties != null) return;

            lock (_thisLock)
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    Initialize(connection);
                }
            }
        }

        /// <summary>
        /// Initialisation of the property to column mapping
        /// </summary>
        /// <param name="conn">SqlConnection</param>
        private static void Initialize(SqlConnection conn)
        {
            //The following code is not fast enough
            //properties = new Dictionary<int, PropertyInfo>();
            //var props = typeof(T).GetProperties();
            //foreach (var prop in props)
            //{
            //    if (GetSchemaTable().Columns.Contains(prop.Name))
            //    {
            //        properties.Add(GetSchemaTable().Columns[prop.Name].Ordinal, prop);
            //    }
            //}

            // optimized using delegates in a static variable
            _properties = new Dictionary<int, Func<T, dynamic>>();
            //get all properties of T
            var props = typeof(T).GetProperties();
            //retrieve schema of table from database
            CreateSchemaTable(conn);
            foreach (var prop in props)
            {
                // check if property is a column in the database and if column is not readonly (computed column)
                if (_schemaTable.Columns.Contains(prop.Name) && !_schemaTable.Columns[prop.Name].ReadOnly)
                {
                    // get the delegate of the property, this is where the magic happens
                    Func<T, dynamic> converted = DynamicProperty(prop);
                    // save the column ordinal and the delegate in the static property variable
                    _properties.Add(_schemaTable.Columns[prop.Name].Ordinal, converted);
                }
            }
        }

        /// <summary>
        /// retrieving the table schema from the database 
        /// </summary>
        /// <param name="conn">SqlConnection</param>
        private static void CreateSchemaTable(SqlConnection conn)
        {
            _schemaTable = new DataTable();
            var adapter = new SqlDataAdapter(string.Format("SELECT * FROM {0}", _destinationTableName), conn);
            adapter.FillSchema(_schemaTable, SchemaType.Source);
        }

        /// <summary>
        /// Current Object
        /// </summary>
        protected T Current
        {
            get
            {
                if (_source == null)
                {
                    throw new InvalidOperationException("The reader is closed");
                }
                return _sourceEnumerator.Current;
            }
        }

        /// <summary>
        /// Name of the Destination table
        /// </summary>
        public string DestinationTableName
        {
            get
            {
                return _destinationTableName;
            }
            set
            {
                _destinationTableName = value;
            }
        }

        /// <summary>
        /// The ColumnMapping
        /// </summary>
        public List<SqlBulkCopyColumnMapping> ColumnMapping
        {
            get
            {
                return _properties.Keys.Select(prop => new SqlBulkCopyColumnMapping(prop, prop)).ToList();
            }
        }

        #region IDataReader Members

        public void Close()
        {
            Dispose();
        }

        public int Depth
        {
            get { return 1; }
        }

        public DataTable GetSchemaTable()
        {
            return _schemaTable.Copy();
        }

        public bool IsClosed
        {
            get
            {
                return _sourceEnumerator == null;
            }
        }

        public bool NextResult()
        {
            return false;
        }

        public bool Read()
        {
            if (_sourceEnumerator == null)
                _sourceEnumerator = _source.GetEnumerator();

            _affectedRows++;
            bool result = _sourceEnumerator.MoveNext();

            if (!result) // reset
            {
                _sourceEnumerator = null;
                _affectedRows = 0;
            }

            return result;
        }

        public int RecordsAffected
        {
            get
            {
                return _affectedRows;
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            //_source = null;
            _sourceEnumerator = null;
        }

        #endregion

        #region IDataRecord Members

        public bool GetBoolean(int i)
        {
            return GetValue(i);
        }

        public byte GetByte(int i)
        {
            return GetValue(i);
        }

        /// <summary>
        /// NotSupported, Not needed for bulkcopy, 
        /// </summary>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public char GetChar(int i)
        {
            return GetValue(i);
        }

        /// <summary>
        /// NotSupported, Not needed for bulkcopy, 
        /// </summary>
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public int FieldCount
        {
            get
            {
                return _properties.Count;
            }
        }

        /// <summary>
        /// NotSupported, Not needed for bulkcopy, 
        /// </summary>
        public IDataReader GetData(int i)
        {
            throw new NotSupportedException();
        }

        public string GetDataTypeName(int i)
        {
            return _schemaTable.Columns[i].DataType.Name;
        }

        public Type GetFieldType(int i)
        {
            return _schemaTable.Columns[i].DataType;
        }

        public string GetName(int i)
        {
            return _schemaTable.Columns[i].ColumnName;
        }

        public int GetOrdinal(string name)
        {
            return _schemaTable.Columns[name].Ordinal;
        }

        public bool IsDBNull(int i)
        {
            return GetValue(i) == null;
        }

        public object this[string name]
        {
            get
            {
                return GetValue(GetOrdinal(name));
            }
        }

        public object this[int i]
        {
            get
            {
                return GetValue(i);
            }
        }

        /// <summary>
        /// NotSupported, Not needed for bulkcopy, 
        /// </summary>
        public int GetValues(object[] values)
        {
            throw new NotSupportedException();
        }

        public DateTime GetDateTime(int i)
        {
            return GetValue(i);
        }

        public decimal GetDecimal(int i)
        {
            return (decimal)GetValue(i);
        }

        public double GetDouble(int i)
        {
            return GetValue(i);
        }

        public float GetFloat(int i)
        {
            return GetValue(i);
        }

        public Guid GetGuid(int i)
        {
            return GetValue(i);
        }

        public short GetInt16(int i)
        {
            return GetValue(i);
        }

        public int GetInt32(int i)
        {
            return GetValue(i);
        }

        public long GetInt64(int i)
        {
            return GetValue(i);
        }

        public string GetString(int i)
        {
            return GetValue(i);
        }

        #endregion

        /// <summary>
        /// retrieves the property value of column index i using the delegates stored in the static properties dictionary
        /// </summary>
        /// <param name="i">column index</param>
        /// <returns></returns>
        public dynamic GetValue(int i)
        {
            return _properties[i](Current);
        }

        /// <summary>
        /// HelperMethod to retrieve a delegate from a property
        /// </summary>
        /// <typeparam name="TIn">object type to get the property from, is always typeof(T)</typeparam>
        /// <typeparam name="TReturn">return type of the property</typeparam>
        /// <param name="method">System.Reflection.PropertyInfo of the property</param>
        /// <returns>delegate</returns>
        static Func<TIn, object> DynamicPropertyHelper<TIn, TReturn>(PropertyInfo method)
        {
            // Convert the slow MethodInfo into a fast, strongly typed, open delegate
            var func = (Func<TIn, TReturn>)Delegate.CreateDelegate(typeof(Func<TIn, TReturn>), method.GetGetMethod());

            // Now create a more weakly typed delegate which will call the strongly typed one
            Func<TIn, dynamic> ret = target => func(target);

            return ret;
        }

        /// <summary>
        /// Retrieves a delegate from a property, uses the MagicPropertyHelper method to do so
        /// </summary>
        /// <param name="prop">System.Reflection.PropertyInfo of the property</param>
        /// <returns>delegate</returns>
        static Func<T, object> DynamicProperty(PropertyInfo prop)
        {
            // First fetch the generic form
            var genericHelper = typeof(BulkCopyListReader<T>).GetMethod("DynamicPropertyHelper", BindingFlags.Static | BindingFlags.NonPublic);

            // Now supply the type arguments
            var constructedHelper = genericHelper.MakeGenericMethod(typeof(T), prop.GetGetMethod().ReturnType);

            // Now call it. The null argument is because it's a static method.
            var ret = constructedHelper.Invoke(null, new object[] { prop });

            // Cast the result to the right kind of delegate and return it
            return (Func<T, dynamic>)ret;
        }
    }
}
