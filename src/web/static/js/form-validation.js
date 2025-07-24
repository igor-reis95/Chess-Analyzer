document.addEventListener('DOMContentLoaded', function () {
    try {
        const input = document.getElementById('username');
        const form = document.querySelector('form');
        const spinner = document.getElementById('loading-spinner');
        const invalidFeedback = input.nextElementSibling;
        const platformRadios = document.querySelectorAll('input[name="platform"]');

        if (!input || !form || !spinner) {
            console.warn('Required elements not found');
            return;
        }

        input.setCustomValidity('');

        function getSelectedPlatform() {
            const selected = Array.from(platformRadios).find(r => r.checked);
            return selected ? selected.value : 'lichess';
        }

        platformRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                // Clear previous errors
                input.classList.remove('is-invalid');
                input.setCustomValidity('');

                // Optionally re-validate username if not empty
                if (input.value.trim()) {
                    input.dispatchEvent(new Event('blur'));
                }
            });
        });

        input.addEventListener('blur', async () => {
            const username = input.value.trim().toLowerCase();
            if (!username) {
                input.classList.remove('is-invalid');
                return;
            }

            const platform = getSelectedPlatform();

            try {
                let apiUrl = '';
                let errorMessage = '';

                if (platform === 'lichess') {
                    apiUrl = `https://lichess.org/api/user/${encodeURIComponent(username)}`;
                    errorMessage = '✗ Username not found on Lichess';
                } else if (platform === 'chesscom') {
                    apiUrl = `https://api.chess.com/pub/player/${encodeURIComponent(username)}`;
                    errorMessage = '✗ Username not found on Chess.com';
                }

                const response = await fetch(apiUrl);

                if (!response.ok) {
                    if (response.status === 404) {
                        input.classList.add('is-invalid');
                        invalidFeedback.textContent = errorMessage;
                        input.setCustomValidity('Invalid username');
                    } else {
                        window.location.href = `/error?message=${encodeURIComponent(platform + ' API error - try again later')}`;
                    }
                    return;
                }

                // User exists
                input.classList.remove('is-invalid');
                input.setCustomValidity('');
            } catch (error) {
                window.location.href = `/error?message=${encodeURIComponent('Network error - please check your connection')}`;
                console.error('Validation error:', error);
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
