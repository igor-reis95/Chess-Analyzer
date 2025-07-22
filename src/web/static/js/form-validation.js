document.addEventListener('DOMContentLoaded', function () {
    try {
        const input = document.getElementById('username');
        const form = document.querySelector('form');
        const spinner = document.getElementById('loading-spinner');
        const invalidFeedback = input.nextElementSibling;

        if (!input || !form || !spinner) {
            console.warn('Required elements not found');
            return;
        }

        // Remove any default validation
        input.setCustomValidity('');

        input.addEventListener('blur', async () => {
            if (!input.value.trim()) {
                input.classList.remove('is-invalid');
                return;
            }

            const username = input.value.trim().toLowerCase();
            
            try {
                const response = await fetch(`https://lichess.org/api/user/${encodeURIComponent(username)}`);
                
                if (!response.ok) {
                    if (response.status === 404) {
                        input.classList.add('is-invalid');
                        invalidFeedback.textContent = '✗ Username not found on Lichess';
                        input.setCustomValidity('Invalid username');
                    } else {
                        window.location.href = `/error?message=${encodeURIComponent('Lichess API error - try again later')}`;
                    }
                    return;
                }

                // User exists
                input.classList.remove('is-invalid');
                input.setCustomValidity('');
            } catch (error) {
                window.location.href = `/error?message=${encodeURIComponent('Network error - please check your connection')}`;
                console.error('Lichess validation error:', error);
            }
        });


        input.addEventListener('input', () => {
            input.classList.remove('is-invalid');
            input.setCustomValidity('');
        });

        form.addEventListener('submit', () => {
            spinner.style.display = 'block';
        });

    } catch (error) {
        window.location.href = `/error?message=${encodeURIComponent('Unexpected error - please try again')}`;
        console.error('Form validation error:', error);
    }
});