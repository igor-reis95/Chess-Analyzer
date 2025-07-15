document.addEventListener('DOMContentLoaded', function () {
    try {
        const input = document.querySelector('input[name="username"]');
        const form = document.querySelector('form');
        const spinner = document.getElementById('loading-spinner');

        if (!input || !form || !spinner) {
            console.warn('Required elements not found');
            return;
        }

        input.addEventListener('input', () => {
            const isValid = input.value.length >= 3;
            input.setCustomValidity(isValid ? "" : "Username must be at least 3 characters");
            input.classList.toggle('is-invalid', !isValid);
            input.classList.toggle('is-valid', isValid);
        });

        form.addEventListener('submit', () => {
            spinner.style.display = 'block';
        });
    } catch (error) {
        console.error('Form validation error:', error);
    }
});