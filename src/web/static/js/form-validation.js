document.addEventListener('DOMContentLoaded', function () {
    try {
        const input = document.getElementById('username');
        const form = document.querySelector('form');
        const spinner = document.getElementById('loading-spinner');
        const submitBtn = form.querySelector('button[type="submit"]');
        const invalidFeedback = input.nextElementSibling;
        const platformRadios = document.querySelectorAll('input[name="platform"]');
        
        let isValidating = false;
        let lastValidation = {
            username: '',
            platform: '',
            valid: false
        };

        if (!input || !form || !spinner || !submitBtn) {
            console.warn('Required elements not found');
            return;
        }

        input.setCustomValidity('');

        function getSelectedPlatform() {
            const selected = Array.from(platformRadios).find(r => r.checked);
            return selected ? selected.value : 'lichess';
        }

        function setFormState(validating, valid = false) {
            isValidating = validating;
            submitBtn.disabled = validating || !valid;
            
            if (validating) {
                submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Validating username...';
            } else {
                submitBtn.textContent = 'Analyze Games';
            }
        }

        platformRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                input.classList.remove('is-invalid');
                input.setCustomValidity('');
                lastValidation.valid = false;
                setFormState(false, false);
                
                if (input.value.trim()) {
                    validateUsername();
                }
            });
        });

        async function validateUsername() {
            const username = input.value.trim().toLowerCase();
            const platform = getSelectedPlatform();
            
            if (!username) {
                input.classList.remove('is-invalid');
                lastValidation.valid = false;
                setFormState(false, false);
                return;
            }

            // Skip if we already validated this username/platform combo
            if (lastValidation.username === username && 
                lastValidation.platform === platform && 
                lastValidation.valid) {
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
                        lastValidation.valid = false;
                    }
                    setFormState(false, false);
                    return;
                }

                // User exists
                input.classList.remove('is-invalid');
                input.setCustomValidity('');
                lastValidation = {
                    username: username,
                    platform: platform,
                    valid: true
                };
                setFormState(false, true);
                
            } catch (error) {
                console.error('Validation error:', error);
                setFormState(false, false);
            }
        }

        // Debounced validation
        let debounceTimer;
        input.addEventListener('input', () => {
            clearTimeout(debounceTimer);
            input.classList.remove('is-invalid');
            input.setCustomValidity('');
            lastValidation.valid = false;
            setFormState(false, false);
        });

        input.addEventListener('blur', () => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(validateUsername, 300);
        });

        form.addEventListener('submit', (e) => {
            if (isValidating || !lastValidation.valid) {
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