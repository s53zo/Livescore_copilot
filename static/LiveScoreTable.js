import React, { useState, useEffect, useMemo } from 'react';

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

    // Setup SSE connection with reconnect logic
    useEffect(() => {
        let eventSource;
        let reconnectTimeout;
        let isMounted = true;

        const connect = () => {
            const url = `/livescore-pilot/events?contest=${contest}&callsign=${callsign}&filter_type=${filterType}&filter_value=${filterValue}`;
            console.log('Attempting to connect to SSE:', url);

            eventSource = new EventSource(url);
            console.log('EventSource created:', eventSource.readyState);

            // Handle initial data
            eventSource.addEventListener('init', (event) => {
                console.log('Received init event:', event);
                if (!isMounted) return;
                try {
                    const data = JSON.parse(event.data);
                    console.log('Parsed init data:', data);
                    setData(data);
                    setLoading(false);
                    setError(null);
                } catch (e) {
                    console.error('Error parsing init data:', e);
                    setError('Failed to parse initial data');
                }
            });

            // Handle updates
            eventSource.addEventListener('update', (event) => {
                console.log('Received update event:', event);
                if (!isMounted) return;
                try {
                    const update = JSON.parse(event.data);
                    console.log('Parsed update data:', update);
                    
                    setData(prevData => {
                        // If this is a full update (initial data)
                        if (update.stations) {
                            return update;
                        }
                        
                        // Apply delta changes to existing stations
                        const updatedStations = prevData.stations.map(station => {
                            const stationUpdate = update.changes.find(change => 
                                change.callsign === station.callsign
                            );
                            
                            if (stationUpdate) {
                                return {
                                    ...station,
                                    ...stationUpdate,
                                    bandData: {
                                        ...station.bandData,
                                        ...(stationUpdate.bandData || {})
                                    }
                                };
                            }
                            return station;
                        });
                        
                        return {
                            ...prevData,
                            timestamp: update.timestamp,
                            stations: updatedStations
                        };
                    });
                    
                    setCountdown(120); // Reset countdown on update
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
                        setLoading(true);
                        connect();
                    }
                }, 5000);
            };
        };

        console.log('Setting up SSE connection...');
        connect();

        // Countdown timer
        const countdownInterval = setInterval(() => {
            setCountdown(prev => {
                if (prev <= 0) return 120;
                return prev - 1;
            });
        }, 1000);

        // Cleanup
        return () => {
            console.log('Cleaning up SSE connection...');
            isMounted = false;
            if (eventSource) {
                console.log('Closing EventSource connection');
                eventSource.close();
            }
            if (reconnectTimeout) clearTimeout(reconnectTimeout);
            clearInterval(countdownInterval);
        };
    }, [contest, callsign, filterType, filterValue]);

        // Countdown timer
        const countdownInterval = setInterval(() => {
            setCountdown(prev => {
                if (prev <= 0) return 120;
                return prev - 1;
            });
        }, 1000);

        return () => {
            isMounted = false;
            if (eventSource) eventSource.close();
            if (reconnectTimeout) clearTimeout(reconnectTimeout);
            clearInterval(countdownInterval);
        };
    }, [contest, callsign, filterType, filterValue]);

    // Memoize the processed stations data
    const processedStations = useMemo(() => 
        data.stations.map((station, index) => ({
            ...station,
            position: index + 1,
            isHighlighted: station.callsign === data.callsign
        })), [data.stations, data.callsign]);

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
            {/* Header Section */}
            <div className="mb-6">
                <h1 className="text-2xl font-bold mb-2">
                    Contest Progress Report - {data.contest}
                </h1>
                <div className="text-sm text-gray-600">
                    Last Updated: {new Date(data.timestamp).toLocaleString()}
                </div>
            </div>

            {/* Countdown Timer */}
            <div className="fixed top-4 right-4 bg-green-500 text-white px-4 py-2 rounded-md">
                Next update in {Math.floor(countdown/60)}:{(countdown%60).toString().padStart(2, '0')}
            </div>

            {/* Score Table */}
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
                        {processedStations.map((station) => (
                            <tr key={station.callsign} 
                                className={station.isHighlighted ? 'bg-green-50' : 'hover:bg-gray-50'}>
                                <td className="px-6 py-4 whitespace-nowrap">{station.position}</td>
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

// Create root element and render
const root = document.getElementById('root');
if (root) {
    ReactDOM.createRoot(root).render(
        <React.StrictMode>
            <LiveScoreTable />
        </React.StrictMode>
    );
}
