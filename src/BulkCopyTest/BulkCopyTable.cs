using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel.DataAnnotations;

namespace BulkCopyTest
{
    public class BulkCopyTable
    {
        [Key]
        public long Id { get; set; }
        public string Name { get; set; }
        public int Counter { get; set; }
        public DateTime DateCreated { get; set; }
    }
}
