using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Isam.Esent.Interop;

namespace EsentTempTableTest
{
    public class SubscriberCursor : ManagedTableCursor<SubscriberConfig>
    {
        public SubscriberCursor(Instance instance, SubscriberConfig config) : base(instance, config)
        {
            // Get the IDs of the columns specific to this implementation 
            _subscriberColumn = Api.GetTableColumnid(_sesid, _table, _config.SubscriberColumnName);
            _channelColumn = Api.GetTableColumnid(_sesid, _table, _config.ChannelColumnName);
        }


        private readonly JET_COLUMNID _subscriberColumn;
        private readonly JET_COLUMNID _channelColumn;

        public IEnumerable<string> GetSubscribersByChannel(string channel)
        {
            // Iterate over all records tagged with the channel
            Api.JetSetCurrentIndex(_sesid, _table, "channelindex");
            Api.MakeKey(_sesid, _table, channel, Encoding.ASCII, MakeKeyGrbit.NewKey);

            var subscribers = new List<string>();
            if (Api.TrySeek(_sesid, _table, SeekGrbit.SeekEQ))
            {
                Api.MakeKey(_sesid, _table, channel, Encoding.ASCII, MakeKeyGrbit.NewKey);
                Api.JetSetIndexRange(_sesid, _table, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

                
                do
                {
                    string subscriber = Api.RetrieveColumnAsString(
                        _sesid,
                        _table,
                        _subscriberColumn,
                        Encoding.ASCII
                    );

                    subscribers.Add(subscriber);

                } while (Api.TryMoveNext(_sesid, _table));
            }

            Api.JetSetCurrentIndex(_sesid, _table, null);
            return subscribers;
        }

        public IEnumerable<string> GetChannelsBySubscriber(string subscriber)
        {
            // Get the existing channels 
            var column = new JET_RETRIEVECOLUMN
            {
                columnid = _channelColumn,
                grbit = RetrieveColumnGrbit.RetrieveTag
            };

            Api.JetRetrieveColumns(_sesid, _table, new[] { column }, 1);

            int count = column.itagSequence;
            var channels = new string[count];
            for (int i = 1; i <= count; i++)
            {
                JET_RETINFO retinfo = new JET_RETINFO { itagSequence = i };
                byte[] data = Api.RetrieveColumn(_sesid, _table, _channelColumn, RetrieveColumnGrbit.None, retinfo);
                var channel = Encoding.ASCII.GetString(data);
                channels[i] = channel;
            }

            return channels;
        }

        /// <summary>
        /// Insert data into the data table. No record with the same key
        /// should exist.
        /// </summary>
        /// <param name="data">The data to add.</param>
        public void Insert(string subscriber, IEnumerable<string> channels)
        {
            Api.JetPrepareUpdate(_sesid, _table, JET_prep.Insert);
            try
            {
                // The easy part set the key
                Api.SetColumn(
                    _sesid,
                    _table,
                    _subscriberColumn,
                    subscriber,
                    Encoding.ASCII
                );

                // Loop through all the channels and create them,
                // as this is a new record there is no need for checks 
                foreach (var channel in channels)
                {
                    // Using tag sequence 0 wil mean the items are 
                    // automatically pushed into the mv-column
                    JET_SETINFO setInfo = new JET_SETINFO();
                    setInfo.itagSequence = 0;

                    // Note the use of ASCII to take up half the space
                    // there is no reason either subscriber or channel
                    // will have more than this
                    byte[] data = Encoding.ASCII.GetBytes(channel);
                    Api.JetSetColumn(
                        _sesid,
                        _table,
                        _channelColumn,
                        data,
                        data.Length,
                        SetColumnGrbit.UniqueMultiValues,
                        setInfo
                    );
                }

                Api.JetUpdate(_sesid, _table);
            }
            catch (Exception)
            {
                Api.JetPrepareUpdate(_sesid, _table, JET_prep.Cancel);
                throw;
            }
        }

        public void Append(IEnumerable<string> channels)
        {
            Api.JetPrepareUpdate(_sesid, _table, JET_prep.Replace);
            try
            {
                // Get the existing channels 
                var column = new JET_RETRIEVECOLUMN
                {
                    columnid = _channelColumn,
                    grbit = RetrieveColumnGrbit.RetrieveTag
                };

                Api.JetRetrieveColumns(_sesid, _table, new[] { column }, 1);

                int count = column.itagSequence;                
                var existing = new string[count];
                for(int i = 1; i <= count; i++)
                {
                    JET_RETINFO retinfo = new JET_RETINFO { itagSequence = i };
                    byte[] data = Api.RetrieveColumn(_sesid, _table, _channelColumn, RetrieveColumnGrbit.None, retinfo);
                    var channel = Encoding.ASCII.GetString(data);
                    existing[i -1] = channel;
                }
                
                // Loop through all the channels and create them,
                // as this is a new record there is no need for checks 
                foreach (var channel in channels)
                {
                    // Only add the new ones 
                    if (Array.IndexOf(existing, channel) > -1)
                        continue;

                    // Using tag sequence 0 wil mean the items are 
                    // automatically pushed into the mv-column
                    count++;
                    JET_SETINFO setInfo = new JET_SETINFO();
                    setInfo.itagSequence = count;

                    // Note the use of ASCII to take up half the space
                    // there is no reason either subscriber or channel
                    // will have more than this
                    byte[] data = Encoding.ASCII.GetBytes(channel);
                    Api.JetSetColumn(
                        _sesid,
                        _table,
                        _channelColumn,
                        data,
                        data.Length,
                        SetColumnGrbit.UniqueMultiValues,
                        setInfo
                    );
                }

                Api.JetUpdate(_sesid, _table);
            }
            catch (Exception)
            {
                Api.JetPrepareUpdate(_sesid, _table, JET_prep.Cancel);
                throw;
            }
        }


        public void RemoveChannel(string channel)
        {
            Api.JetPrepareUpdate(_sesid, _table, JET_prep.Replace);
            try
            {
                // Get the existing channels 
                var column = new JET_RETRIEVECOLUMN
                {
                    columnid = _channelColumn,
                    grbit = RetrieveColumnGrbit.RetrieveTag
                };

                Api.JetRetrieveColumns(_sesid, _table, new[] { column }, 1);

                int count = column.itagSequence; 
                for (int i = 1; i <= count; i++)
                {
                    JET_RETINFO retinfo = new JET_RETINFO { itagSequence = i };
                    byte[] data = Api.RetrieveColumn(_sesid, _table, _channelColumn, RetrieveColumnGrbit.None, retinfo);
                    if (channel.Equals(Encoding.ASCII.GetString(data), StringComparison.OrdinalIgnoreCase))
                    {
                        JET_SETINFO setInfo = new JET_SETINFO();
                        setInfo.itagSequence = i;
                        Api.JetSetColumn(
                            _sesid,
                            _table,
                            _channelColumn,
                            null,
                            0,
                            SetColumnGrbit.None,
                            setInfo
                        );
                        break;
                    }                    
                }                 

                Api.JetUpdate(_sesid, _table);
            }
            catch (Exception)
            {
                Api.JetPrepareUpdate(_sesid, _table, JET_prep.Cancel);
                throw;
            }
        }

    }
}
