namespace NEventStore.Persistence.Sql.SqlDialects
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Transactions;
    using NEventStore.Persistence.Sql;
    using System.Threading.Tasks;

    public class DelimitedDbStatement : CommonDbStatement
    {
        private const string Delimiter = ";";

        public DelimitedDbStatement(
            ISqlDialect dialect,
            TransactionScope scope,
            IDbAsyncConnection connection,
            IDbTransaction transaction)
            : base(dialect, scope, connection, transaction)
        {}

        public override async Task<int> ExecuteNonQuery(string commandText)
        {
            var result = 0;
            foreach (var x in SplitCommandText(commandText))
            {
                result += await base.ExecuteNonQuery(x);
            }
            return result;
        }

        private static IEnumerable<string> SplitCommandText(string delimited)
        {
            if (string.IsNullOrEmpty(delimited))
            {
                return new string[] {};
            }

            return delimited.Split(Delimiter.ToCharArray(), StringSplitOptions.RemoveEmptyEntries)
                            .AsEnumerable().Select(x => x + Delimiter)
                            .ToArray();
        }
    }
}