class LiveScoreBoard extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            scores: [],
            lastUpdate: null
        };
        
        // Initialize WebSocket connection
        this.socket = io();
        this.setupWebSocket();
    }

    setupWebSocket() {
        const { initialParams } = this.props;
        
        // Handle WebSocket connection
        this.socket.on('connect', () => {
            console.log('Connected to WebSocket server');
            this.socket.emit('subscribe', {
                contest: initialParams.contest,
                callsign: initialParams.callsign
            });
        });

        // Handle score updates
        this.socket.on('update', (data) => {
            console.log('Score update received:', data);
            this.setState(prevState => ({
                scores: [data, ...prevState.scores],
                lastUpdate: new Date().toLocaleTimeString()
            }));
        });

        // Handle errors
        this.socket.on('error', (error) => {
            console.error('WebSocket error:', error);
        });
    }

    componentWillUnmount() {
        // Clean up WebSocket connection
        this.socket.disconnect();
    }

    render() {
        const { scores, lastUpdate } = this.state;
        const { initialParams } = this.props;

        return (
            <div className="score-board">
                <h1>Contest Progress: {initialParams.contest}</h1>
                <h2>Callsign: {initialParams.callsign}</h2>
                <p>Last update: {lastUpdate || 'No updates yet'}</p>
                
                <div className="scores-container">
                    {scores.map((score, index) => (
                        <div key={index} className="score-item">
                            <p>Score: {score.score}</p>
                            <p>QSOs: {score.qsos}</p>
                            <p>Multipliers: {score.multipliers}</p>
                        </div>
                    ))}
                </div>
            </div>
        );
    }
}

// Make component available globally
window.LiveScoreBoard = LiveScoreBoard;
