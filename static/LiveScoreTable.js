import React, { useState, useEffect } from 'react';

const LiveScoreTable = () => {
    const [data, setData] = useState({
        contest: '',
        callsign: '',
        stations: [],
        timestamp: ''
    });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [countdown, setCountdown] = useState(120);

    // Get URL parameters
    const urlParams = new URLSearchParams(window.location.search);
    const contest = urlParams.get('contest');
    const callsign = urlParams.get('callsign');
    const filterType = urlParams.get('filter_type');
    const filterValue = urlParams.get('filter_value');

    useEffect(() => {
        let eventSource;
        let reconnectTimeout;
        let isMounted = true;

        const connect = () => {
            const url = `/livescore-pilot/events?contest=${contest}&callsign=${callsign}&filter_type=${filterType}&filter_value=${filterValue}`;
            console.log('Connecting to SSE:', url);

            eventSource = new EventSource(url);
            
            // Handle initial data
            eventSource.addEventListener('init', (event) => {
                if (!isMounted) return;
                try {
                    const newData = JSON.parse(event.data);
                    console.log('Parsed initial data:', newData);
                    if (newData && newData.stations) {
                        setData(newData);
                        setLoading(false);
                        setError(null);
                    }
                } catch (e) {
                    console.error('Error parsing initial data:', e);
                    setError('Failed to parse initial data');
                }
            });

            // Handle updates
            eventSource.addEventListener('update', (event) => {
                if (!isMounted) return;
                try {
                    const newData = JSON.parse(event.data);
                    console.log('Received update:', newData);
                    if (newData && newData.stations) {
                        setData(newData);
                        setCountdown(120);
                    }
                } catch (e) {
                    console.error('Error processing update:', e);
                }
            });

            // Handle connection open
            eventSource.onopen = () => {
                console.log('SSE connection opened');
                setError(null);
            };

            // Handle errors
            eventSource.onerror = (err) => {
                console.error('SSE connection error:', err);
                if (!isMounted) return;
                eventSource.close();
                setError('Connection lost - attempting to reconnect...');
                reconnectTimeout = setTimeout(() => {
                    if (isMounted) {
                        console.log('Attempting to reconnect...');
                        setError(null);
                        connect();
                    }
                }, 5000);
            };
        };

        connect();

        // Countdown timer
        const countdownInterval = setInterval(() => {
            setCountdown(prev => Math.max(0, prev - 1));
        }, 1000);

        return () => {
            isMounted = false;
            if (eventSource) {
                console.log('Closing EventSource connection');
                eventSource.close();
            }
            if (reconnectTimeout) clearTimeout(reconnectTimeout);
            clearInterval(countdownInterval);
        };
    }, [contest, callsign, filterType, filterValue]);

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="text-xl text-gray-600">Loading...</div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="bg-red-50 border border-red-200 text-red-700 p-4 rounded-md">
                <h3 className="text-lg font-semibold">Error</h3>
                <p>{error}</p>
            </div>
        );
    }

    return (
        <div className="w-full">
            <div className="mb-6">
                <h1 className="text-2xl font-bold mb-2">
                    Contest Progress Report - {data.contest}
                </h1>
                <div className="text-sm text-gray-600">
                    Last Updated: {new Date(data.timestamp).toLocaleString()}
                </div>
            </div>

            <div className="fixed top-4 right-4 bg-green-500 text-white px-4 py-2 rounded-md">
                Next update in {Math.floor(countdown/60)}:{(countdown%60).toString().padStart(2, '0')}
            </div>

            <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                        <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Pos</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Callsign</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
                            <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Score</th>
                            {['160', '80', '40', '20', '15', '10'].map(band => (
                                <th key={band} className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">
                                    {band}m
                                </th>
                            ))}
                            <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Total</th>
                            <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">Last Update</th>
                        </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                        {data.stations.map((station, index) => (
                            <tr key={station.callsign} 
                                className={station.callsign === data.callsign ? 'bg-green-50' : 'hover:bg-gray-50'}>
                                <td className="px-6 py-4 whitespace-nowrap">{index + 1}</td>
                                <td className="px-6 py-4 whitespace-nowrap font-mono">{station.callsign}</td>
                                <td className="px-6 py-4 whitespace-nowrap">
                                    <div className="flex space-x-2">
                                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium
                                            ${station.category === 'SOA' ? 'bg-blue-100 text-blue-800' :
                                              station.category === 'SO' ? 'bg-purple-100 text-purple-800' :
                                              station.category === 'M/S' ? 'bg-yellow-100 text-yellow-800' :
                                              'bg-green-100 text-green-800'}`}>
                                            {station.category}
                                        </span>
                                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium
                                            ${station.power === 'HIGH' ? 'bg-red-100 text-red-800' :
                                              station.power === 'LOW' ? 'bg-green-100 text-green-800' :
                                              'bg-orange-100 text-orange-800'}`}>
                                            {station.power[0]}
                                        </span>
                                    </div>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-right font-mono">
                                    {station.score.toLocaleString()}
                                </td>
                                {['160', '80', '40', '20', '15', '10'].map(band => (
                                    <td key={band} className="px-6 py-4 whitespace-nowrap text-center font-mono">
                                        {station.bandData[band] || '-'}
                                    </td>
                                ))}
                                <td className="px-6 py-4 whitespace-nowrap text-center font-mono">
                                    {station.totalQsos}/{station.multipliers}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-center">
                                    {new Date(station.lastUpdate).toLocaleTimeString()}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default LiveScoreTable;