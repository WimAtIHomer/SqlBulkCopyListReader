using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using IHomer.Common.BulkCopy;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;

namespace BulkCopyTest
{
    /// <summary>
    /// Test class for testing the BulkCopyListReader
    /// </summary>
    [TestClass]
    public class BulkCopyTest
    {
        /// <summary>
        /// Test method for testing the BulkCopy insert speed
        /// </summary>
        [TestMethod]
        public void TestBulkInsert()
        {
            var list = new List<BulkCopyTable>();
            for (var i = 0; i < 100000; i++)
            {
                var bulkcopyTable = new BulkCopyTable {Counter = i, Name = "Test", DateCreated = DateTime.Now};
                list.Add(bulkcopyTable);
            }

            var watch = new Stopwatch();
            var connection = ConfigurationManager.ConnectionStrings["TestDB"].ConnectionString;
            var reader = new BulkCopyListReader<BulkCopyTable>(list, connection);
            watch.Start();
            BulkCopy(reader, connection);
            watch.Stop();
            Console.WriteLine("ElapsedMiliseconds: {0}", watch.ElapsedMilliseconds);
            Assert.IsTrue(watch.ElapsedMilliseconds < 1200, string.Format("It takes to long: {0} ms", watch.ElapsedMilliseconds));
        }

        /// <summary>
        /// final call to SqlBulkCopy.WriteToServer after setting the ColumnMappings and the DestinationTable
        /// </summary>
        /// <param name="reader">BulkCopyDataReader</param>
        /// <param name="connection">ConnectionString</param>
        private static void BulkCopy(IBulkCopyDataReader reader, string connection)
        {
            using (var copy = new SqlBulkCopy(connection))
            {
                foreach (var map in reader.ColumnMapping)
                {
                    copy.ColumnMappings.Add(map);
                }
                copy.DestinationTableName = reader.DestinationTableName;
                copy.WriteToServer(reader);
            }
        }
    }
}
