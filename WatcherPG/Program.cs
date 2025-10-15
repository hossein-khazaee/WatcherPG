using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

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
