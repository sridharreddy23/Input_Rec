$(document).ready(function() {
    const form = $('#processForm');
    const statusDiv = $('#status');
    const statusText = $('#statusText');
    const progressBar = $('.progress-bar');

    form.on('submit', function(e) {
        e.preventDefault();
        
        // Show loading state
        statusDiv.removeClass('d-none');
        progressBar.css('width', '0%');
        
        const formData = {
            start_time: $('#start_time').val(),
            end_time: $('#end_time').val()
        };

        // Simple validation
        if (!formData.start_time || !formData.end_time) {
            updateStatus('Please fill in all fields', 'error');
            return;
        }

        // Send request to server
        fetch('/process', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: new URLSearchParams(formData)
        })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                updateStatus('Processing completed successfully!', 'success');
                // Simulate progress for demo
                simulateProgress(100);
            } else {
                throw new Error(data.message || 'An error occurred');
            }
        })
        .catch(error => {
            updateStatus('Error: ' + error.message, 'error');
        });
    });

    function updateStatus(message, type = 'info') {
        statusText.text(message);
        // You can add different styling based on message type if needed
    }

    function simulateProgress(target) {
        let width = 0;
        const interval = setInterval(() => {
            if (width >= target) {
                clearInterval(interval);
                return;
            }
            width++;
            progressBar.css('width', width + '%');
            progressBar.attr('aria-valuenow', width);
        }, 20);
    }
});
