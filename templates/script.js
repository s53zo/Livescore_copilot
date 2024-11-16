// script.js

// React component for the QSO Rate Tooltip
const QsoRateTooltip = React.memo(({ callsign, contest, timestamp }) => {
    const [isVisible, setIsVisible] = React.useState(false);
    const [data, setData] = React.useState(null);
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState(null);

    const bandColors = {
        '160m': '#8884d8',
        '80m': '#82ca9d',
        '40m': '#ffc658',
        '20m': '#ff7300',
        '15m': '#ff0000',
        '10m': '#0088ff',
    };

    React.useEffect(() => {
        const fetchRateData = async () => {
            if (!isVisible || data) return;

            setLoading(true);
            setError(null);

            try {
                const params = new URLSearchParams({
                    callsign,
                    contest,
                    timestamp
                });

                const response = await fetch(`/livescore-pilot/api/rates?${params}`);
                if (!response.ok) throw new Error('Failed to fetch rate data');

                const rateData = await response.json();
                setData(rateData);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchRateData();
    }, [isVisible, callsign, contest, timestamp]);

    const CustomTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            return React.createElement('div', {
                className: 'tooltip-content'
            }, [
                React.createElement('p', { key: 'time', className: 'tooltip-label' }, label),
                ...payload.map(entry =>
                    React.createElement('p', {
                        key: entry.name,
                        style: { color: entry.color }
                    }, `${entry.name}: ${entry.value}/hr`)
                )
            ]);
        }
        return null;
    };

    return React.createElement('div', {
        className: 'relative inline-block',
        onMouseEnter: () => setIsVisible(true),
        onMouseLeave: () => setIsVisible(false)
    }, [
        callsign,
        isVisible && React.createElement('div', {
            key: 'tooltip',
            className: 'tooltip-container'
        }, [
            React.createElement('div', {
                key: 'title',
                className: 'tooltip-title'
            }, `${callsign} Rate History`),

            loading && React.createElement('div', {
                key: 'loading',
                className: 'tooltip-loading'
            }, 'Loading rate data...'),

            error && React.createElement('div', {
                key: 'error',
                className: 'tooltip-error'
            }, `Error: ${error}`),

            data && !loading && !error && React.createElement('div', {
                key: 'chart',
                className: 'tooltip-chart'
            }, [
                React.createElement(Recharts.ResponsiveContainer, {
                    width: '100%',
                    height: 200
                }, [
                    React.createElement(Recharts.LineChart, {
                        data: data.rates,
                        margin: { top: 5, right: 5, bottom: 5, left: 5 }
                    }, [
                        React.createElement(Recharts.XAxis, {
                            dataKey: 'time',
                            stroke: '#666',
                            fontSize: 12
                        }),
                        React.createElement(Recharts.YAxis, {
                            stroke: '#666',
                            fontSize: 12
                        }),
                        React.createElement(Recharts.Tooltip, {
                            content: CustomTooltip
                        }),
                        React.createElement(Recharts.Legend),
                        ...Object.keys(bandColors).map(band =>
                            React.createElement(Recharts.Line, {
                                key: band,
                                type: 'monotone',
                                dataKey: band,
                                name: band,
                                stroke: bandColors[band],
                                strokeWidth: 2,
                                dot: false,
                                connectNulls: true
                            })
                        )
                    ])
                ]),
                React.createElement('div', {
                    key: 'summary',
                    className: 'tooltip-summary'
                }, Object.entries(data.totalQsos).map(([band, qsos]) =>
                    React.createElement('div', {
                        key: band,
                        className: 'tooltip-band-summary'
                    }, [
                        React.createElement('span', { key: 'band' }, band),
                        React.createElement('span', { key: 'qsos' }, `${qsos} QSOs`)
                    ])
                ))
            ])
        ])
    ]);
});

// Initialize tooltips on DOMContentLoaded
document.addEventListener('DOMContentLoaded', () => {
    // Find all tooltip containers
    document.querySelectorAll('.rate-tooltip').forEach(element => {
        const props = {
            callsign: element.dataset.callsign,
            contest: element.dataset.contest,
            timestamp: element.dataset.timestamp
        };

        // Render the React tooltip component
        ReactDOM.render(
            React.createElement(QsoRateTooltip, props),
            element
        );
    });
});

// Refresh page every 2 minutes
function refreshPage() {
    const params = new URLSearchParams(window.location.search);
    window.location.href = '/reports/live.html?' + params.toString();
}

setInterval(refreshPage, 120000);

// Update countdown timer and relative times
function updateCountdown() {
    const countdownElement = document.getElementById('countdown');
    let minutes = 1;
    let seconds = 59;

    function pad(num) {
        return num.toString().padStart(2, '0');
    }

    function updateRelativeTimes() {
        document.querySelectorAll('.relative-time').forEach(el => {
            const timestamp = new Date(el.dataset.timestamp + 'Z');
            const now = new Date();
            const diff = Math.floor((now - timestamp) / 1000 / 60);

            if (diff < 60) {
                el.textContent = `${diff}m ago`;
            } else if (diff < 1440) {
                el.textContent = `${Math.floor(diff / 60)}h ${diff % 60}m ago`;
            } else {
                el.textContent = Math.floor(diff / 1440) + 'd ago`;
            }
        });
    }

    const timer = setInterval(() => {
        if (minutes === 0 && seconds === 0) {
            clearInterval(timer);
            return;
        }

        if (seconds === 0) {
            minutes--;
            seconds = 59;
        } else {
            seconds--;
        }

        countdownElement.textContent = `${minutes}:${pad(seconds)}`;
        updateRelativeTimes();
    }, 1000);

    updateRelativeTimes();
}

document.addEventListener('DOMContentLoaded', updateCountdown);

