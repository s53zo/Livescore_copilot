<!DOCTYPE html>
<html>
<head>
    <title>Contest Progress Report - {contest}</title>
    <script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <!-- Add other required scripts -->
</head>
<body>
    <div id="root"></div>
    <script>
        const initialParams = {
            callsign: '{callsign}',
            contest: '{contest}',
            category_filter: '{category_filter}',
            filter_type: '{filter_type}',
            filter_value: '{filter_value}'
        };
        
        ReactDOM.render(
            React.createElement(LiveScoreBoard, { initialParams }),
            document.getElementById('root')
        );
    </script>
</body>
</html>

