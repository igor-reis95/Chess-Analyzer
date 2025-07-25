// Get platform radio buttons and username input
const platformRadios = document.querySelectorAll('input[name="platform"]');
const usernameInput = document.getElementById('username');

// Default usernames for each platform
const DEFAULT_USERNAMES = {
    'lichess.org': 'IgorSReis',
    'chess.com': 'samukaunt1'
};

// Add change event listeners to platform radios
platformRadios.forEach(radio => {
radio.addEventListener('change', function() {
    if (this.checked) {
    usernameInput.value = DEFAULT_USERNAMES[this.value];
    }
});
});