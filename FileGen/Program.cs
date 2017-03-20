using System;
using System.Collections.Generic;
using System.Linq;

namespace ConsoleApplication
{
    public class Program
    {
            
        public static int amountOfRecords = 10;
        public static int numberOfPathways = 1;
        public static int numberOfPeople = 3;
        static Random rand = new Random();
        public static void Main(string[] args)
        {
            var expectedState = CreateOutputFile();

            var filePath = System.IO.Path.GetTempFileName();
            System.Console.WriteLine("Output file: " + filePath);
            var expectedResult = System.IO.File.Create(filePath);

            using (var file = new System.IO.StreamWriter(expectedResult))
            {
                foreach (var person in expectedState.OrderBy(per => per.UpdatedIndex))
                {
                    file.WriteLine(person.ToString());
                }
            }
        }

        public static HashSet<Person> CreateOutputFile()
        {
            var hashSet = new HashSet<Person>();

            var filePath = System.IO.Path.GetTempFileName();
            System.Console.WriteLine("Input file: " + filePath);
            var inputFile = System.IO.File.Create(filePath);

            using (var file = new System.IO.StreamWriter(inputFile))
            {
                for (var i = 0; i < amountOfRecords; i++)
                {
                    var person = GeneratePerson(i);

                    file.WriteLine(person.ToString());

                    //expecting error
                    var matches = hashSet.Where(item => item.NhsNumber == person.NhsNumber
                                            && (item.PatientPathway == person.PatientPathway ||
                                            item.ReferralStart == person.ReferralStart ||
                                            item.TreatmentStart == person.TreatmentStart));

                    if (matches.Count() == 1)
                    {
                        var match = matches.First();
                        match.Age = person.Age ?? match.Age;
                        match.Address = person.Address ?? match.Address;
                        match.ReferralStart = person.ReferralStart ?? match.ReferralStart;
                        match.TreatmentStart = person.TreatmentStart ?? match.TreatmentStart;
                        match.PatientPathway = person.PatientPathway ?? match.PatientPathway;

                        match.UpdatedIndex = i;
                    }
                    else if (matches.Count() > 1)
                    {
                        throw new NotImplementedException();
                        foreach (var match in matches)
                        {
                            match.Errored = true;
                            match.UpdatedIndex = i;
                        }
                    }
                    else
                    {
                        hashSet.Add(person);
                    }
                }
            }

            return hashSet;
        }

        public static Person GeneratePerson(int index)
        {
            return new Person(ranStr(numberOfPeople), RandBool() ? (int?)rand.Next(10, 100) : null, RandBool() ? Guid.NewGuid().ToString() : null, ranStr(numberOfPathways), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), index);
        }

        public static string ranStr(int max)
        {
            return rand.Next(max).ToString();
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
            public string PatientPathway;
            public string ReferralStart;
            public string TreatmentStart;
            public bool Errored;
            public int UpdatedIndex;

            public Person(string nhsNumber, int? age, string address, string patientPathway, string referralStart, string treatmentStart, int updatedIndex)
            {
                this.NhsNumber = nhsNumber;
                this.Age = age;
                this.Address = address;
                this.UpdatedIndex = updatedIndex;
                ReferralStart = referralStart;
                TreatmentStart = treatmentStart;
                PatientPathway = patientPathway;
            }

            public override String ToString()
            {
                return $"{NhsNumber},{Age},{Address},{PatientPathway},{ReferralStart},{TreatmentStart},{Errored}";
            }
        }
    }
}