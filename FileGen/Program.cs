using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsoleApplication
{
    public class Program
    {
        static Random rand = new Random();
        public static void Main(string[] args)
        {
            var expectedState = CreateOutputFile();

            var filePath = System.IO.Path.GetTempFileName();
            System.Console.WriteLine("Output file: " + filePath);
            var expectedResult = System.IO.File.Create(filePath);

            using (var file = new System.IO.StreamWriter(expectedResult))
            {
                foreach (var person in expectedState.Values.OrderBy(per => per.UpdatedIndex))
                {
                    file.WriteLine(person.ToString());                    
                }
            }
        }

        public static Dictionary<string, Person> CreateOutputFile()
        {
            var dict = new Dictionary<string, Person>();

            var filePath = System.IO.Path.GetTempFileName();
            System.Console.WriteLine("Input file: " + filePath);
            var inputFile = System.IO.File.Create(filePath);

            using (var file = new System.IO.StreamWriter(inputFile))
            {
                for (var i = 0; i < 1000000; i++)
                {
                    var person = GeneratePerson(i);

                    file.WriteLine(person.ToString());

                    if (dict.ContainsKey(person.NhsNumber))
                    {
                        var stored = dict[person.NhsNumber];

                        stored.Age = person.Age ?? stored.Age;
                        stored.Address = person.Address ?? stored.Address;
                        stored.UpdatedIndex = i;
                    } else {
                        dict.Add(person.NhsNumber, person);
                    }
                }
            }

            return dict;
        }

        public static Person GeneratePerson(int index)
        {
            return new Person(rand.Next(150).ToString(), RandBool() ? (int?)rand.Next(10, 100) : null, RandBool() ? Guid.NewGuid().ToString() : null, index);
        }

        public static bool RandBool()
        {
            return rand.Next(2) == 0;
        }
        public class Person
        {
            public string NhsNumber;
            public int? Age;
            public string Address;
            public int UpdatedIndex;

            public Person(string nhsNumber, int? age, string address, int updatedIndex)
            {
                this.NhsNumber = nhsNumber;
                this.Age = age;
                this.Address = address;
                this.UpdatedIndex = updatedIndex;
            }

            public override String ToString()
            {
                return $"{NhsNumber},{Age},{Address}";
            }
        }
    }
}