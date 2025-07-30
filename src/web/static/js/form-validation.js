document.addEventListener('DOMContentLoaded', function () {
    try {
        const input = document.getElementById('username');
        const form = document.querySelector('form');
        const spinner = document.getElementById('loading-spinner');
        const submitBtn = form.querySelector('button[type="submit"]');
        const invalidFeedback = input.nextElementSibling;
        const platformRadios = document.querySelectorAll('input[name="platform"]');
        
        const DEFAULT_USERNAME = "IgorSReis";
        let isValidating = false;

        if (!input || !form || !spinner || !submitBtn) {
            console.warn('Required elements not found');
            return;
        }

        function getSelectedPlatform() {
            const selected = Array.from(platformRadios).find(r => r.checked);
            return selected ? selected.value : 'lichess';
        }

        function setFormState(validating, valid = false) {
            isValidating = validating;
            submitBtn.disabled = validating || !valid;
            
            if (validating) {
                submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Validating...';
            } else {
                submitBtn.textContent = 'Analyze Games';
            }
        }

        // Skip validation for default username
        function shouldSkipValidation() {
            return input.value.trim() === DEFAULT_USERNAME;
        }

        async function validateUsername() {
            if (shouldSkipValidation()) {
                setFormState(false, true);
                return;
            }

            const username = input.value.trim().toLowerCase();
            const platform = getSelectedPlatform();
            
            if (!username) {
                setFormState(false, false);
                return;
            }

            setFormState(true);
            
            try {
                let apiUrl = '';
                let errorMessage = '';

                if (platform === 'lichess.org') {
                    apiUrl = `https://lichess.org/api/user/${encodeURIComponent(username)}`;
                    errorMessage = '✗ Username not found on Lichess';
                } else if (platform === 'chess.com') {
                    apiUrl = `https://api.chess.com/pub/player/${encodeURIComponent(username)}`;
                    errorMessage = '✗ Username not found on Chess.com';
                }

                const response = await fetch(apiUrl);

                if (!response.ok) {
                    if (response.status === 404) {
                        input.classList.add('is-invalid');
                        invalidFeedback.textContent = errorMessage;
                        input.setCustomValidity('Invalid username');
                    }
                    setFormState(false, false);
                    return;
                }

                // User exists
                input.classList.remove('is-invalid');
                input.setCustomValidity('');
                setFormState(false, true);
                
            } catch (error) {
                console.error('Validation error:', error);
                setFormState(false, false);
            }
        }

        // Clear validation when platform changes
        platformRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                input.classList.remove('is-invalid');
                input.setCustomValidity('');
                if (!shouldSkipValidation() && input.value.trim()) {
                    validateUsername();
                }
            });
        });

        // Validate on blur (except for default username)
        input.addEventListener('blur', () => {
            if (!shouldSkipValidation()) {
                validateUsername();
            }
        });

        form.addEventListener('submit', (e) => {
            if (shouldSkipValidation()) {
                spinner.style.display = 'block';
                return;
            }
            
            if (isValidating || input.classList.contains('is-invalid')) {
                e.preventDefault();
                validateUsername();
            } else {
                spinner.style.display = 'block';
            }
        });

    } catch (error) {
        console.error('Form validation error:', error);
    }
});