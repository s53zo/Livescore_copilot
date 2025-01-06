// LiveScoreTable.js
import React, { useState, useEffect, useMemo } from 'react';

// Main component
const LiveScoreTable = () => {
    const [data, setData] = useState({
        contest: '',
        callsign: '',
        stations: [],
        timestamp: ''
    });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    // Get URL parameters
    const urlParams = new URLSearchParams(window.location.search);
    const contest = urlParams.get('contest');
    const callsign = urlParams.get('callsign');
    const filterType = urlParams.get('filter_type');
    const filterValue = urlParams.get('filter_value');

    // Fetch data function
    const fetchData = async () => {
        try {
            const response = await fetch(`/livescore-pilot/api/scores?contest=${contest}&callsign=${callsign}&filter_type=${filterType}&filter_value=${filterValue}`);
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            const jsonData = await response.json();
            setData(jsonData);
            setLoading(false);
            // Data updated successfully
        } catch (err) {
            setError(err.message);
            setLoading(false);
        }
    };

    // Initial data fetch
    useEffect(() => {
        fetchData();
        
        // Set up auto-refresh
        const intervalId = setInterval(fetchData, 120000); // 2 minutes
        
        return () => {
            clearInterval(intervalId);
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
