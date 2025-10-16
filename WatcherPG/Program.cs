using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using static System.Net.Mime.MediaTypeNames;

class Program
{
    private const string ConnString = "Host=172.19.0.1;Port=5433;Username=postgres;Password=12345;Database=accountingdb";
    private const string SlotName = "cdc_slot";
    private const string PublicationName = "accounting_pub";
    private const string LsnFilePath = "lsn.txt";

    static async Task Main()
    {
        var savedLsn = LoadLsn();

        await using var conn = new LogicalReplicationConnection(ConnString);
        await conn.Open();

        var slot = new PgOutputReplicationSlot(SlotName);
        var options = new PgOutputReplicationOptions(
            protocolVersion: 1,
            binary: false,
            publicationNames: new[] { PublicationName }
        );
        await foreach (var msg in conn.StartReplication(slot,options,CancellationToken.None,walLocation: savedLsn))
        {
            switch (msg)
            {
                case InsertMessage insert:
                    Console.WriteLine($"\n[INSERT] Table: {insert.Relation.RelationName}");
                    int i = 0;
                    await foreach (var value in insert.NewRow)
                    {
                        var col = insert.Relation.Columns[i++];
                        Console.WriteLine($"{col.ColumnName} = {value.Get<string>()}");
                    }
                    break;

                case UpdateMessage update:
                    Console.WriteLine($"\n[UPDATE] Table: {update.Relation.RelationName}");
                    int j = 0;
                    await foreach (var val in update.NewRow)
                    {
                        var col = update.Relation.Columns[j++];
                        Console.WriteLine($"{col.ColumnName} = {val.Get<string>()}");
                    }
                    break;

                case DeleteMessage delete:
                    var rel = delete.Relation;
                    Console.WriteLine($"\n[DELETE] Table: {rel.RelationName}");
                    break;
            }

            SaveLsn(msg.WalEnd);

            await conn.SendStatusUpdate();
        }
    }

    static void SaveLsn(NpgsqlLogSequenceNumber lsn)
    {
        File.WriteAllText(LsnFilePath, lsn.ToString());
    }

    static NpgsqlLogSequenceNumber? LoadLsn()
    {
        if (!File.Exists(LsnFilePath))
            return null;

        var text = File.ReadAllText(LsnFilePath).Trim();
        return NpgsqlLogSequenceNumber.Parse(text);
    }
}


public static class DataHashHelper
{
    static string key = "PowerKEYAddHere";
    private static readonly byte[] Key = new byte[32];
    private static readonly byte[] IV = new byte[16];


    public static string EncryptPhoneNum(this string phoneNum)
    {
        if (string.IsNullOrEmpty(phoneNum))
            return null;

        Array.Copy(Encoding.UTF8.GetBytes(key), Key, 32);
        Array.Copy(Encoding.UTF8.GetBytes(key), IV, 16);
        using (Aes aes = Aes.Create())
        {
            aes.Key = Key;
            aes.IV = IV;

            using (ICryptoTransform encryptor = aes.CreateEncryptor(aes.Key, aes.IV))
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    using (CryptoStream cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter sw = new StreamWriter(cs))
                        {
                            sw.Write(phoneNum);
                        }
                        return Convert.ToBase64String(ms.ToArray());
                    }
                }
            }
        }
    }
    public static string DecryptPhoneNum(this string phoneNum)
    {
        if (string.IsNullOrEmpty(phoneNum))
            return null;

        using (Aes aes = Aes.Create())
        {
            aes.Key = Key;
            aes.IV = IV;

            using (ICryptoTransform decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
            {
                using (MemoryStream ms = new MemoryStream(Convert.FromBase64String(phoneNum)))
                {
                    using (CryptoStream cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader sr = new StreamReader(cs))
                        {
                            return sr.ReadToEnd();
                        }
                    }
                }
            }
        }
    }
}


/*
 *  
--فعال‌سازی full identity برای CDC کامل
ALTER TABLE "user" REPLICA IDENTITY FULL;
ALTER TABLE "role" REPLICA IDENTITY FULL;

--ساخت publication مخصوص Core
CREATE PUBLICATION core_pub FOR TABLE "user", "role";

--ساخت replication slot برای CDC
SELECT pg_create_logical_replication_slot('core_slot', 'pgoutput');

--بررسی فعال بودن CDC
SELECT * FROM pg_publication;
SELECT* FROM pg_replication_slots;



--------------------------------------------------------------------------
--تنظیم for safety
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

--برای جداول حساس
ALTER TABLE "user" REPLICA IDENTITY FULL;
ALTER TABLE role REPLICA IDENTITY FULL;

--ساخت Publication محدود
CREATE PUBLICATION core_pub FOR TABLE "user", role;

--ساخت Slot مخصوص CDC
SELECT pg_create_logical_replication_slot('core_slot', 'wal2json');


 -- تنظیم for safety
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- برای جداول حساس
ALTER TABLE "user" REPLICA IDENTITY FULL;
ALTER TABLE role REPLICA IDENTITY FULL;

-- ساخت Publication محدود
CREATE PUBLICATION core_pub FOR TABLE "user", role;

-- ساخت Slot مخصوص CDC
SELECT pg_create_logical_replication_slot('core_slot', 'wal2json');




 -- فعال‌سازی full identity برای CDC کامل
ALTER TABLE "user" REPLICA IDENTITY FULL;
ALTER TABLE "role" REPLICA IDENTITY FULL;

-- ساخت publication مخصوص Core
CREATE PUBLICATION core_pub FOR TABLE "user", "role";

-- ساخت replication slot برای CDC
SELECT pg_create_logical_replication_slot('core_slot', 'pgoutput');

-- بررسی فعال بودن CDC
SELECT * FROM pg_publication;
SELECT * FROM pg_replication_slots;



--------------------------------------------------------------------------
-- تنظیم for safety
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- برای جداول حساس
ALTER TABLE "user" REPLICA IDENTITY FULL;
ALTER TABLE role REPLICA IDENTITY FULL;

-- ساخت Publication محدود
CREATE PUBLICATION core_pub FOR TABLE "user", role;

-- ساخت Slot مخصوص CDC
SELECT pg_create_logical_replication_slot('core_slot', 'wal2json');


docker exec -it mongo1 mongosh --eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'172.18.0.1:27017'}]})"

 */
