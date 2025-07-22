document.addEventListener('DOMContentLoaded', function() {
    const gamesInput = document.getElementById('max_games');
    const timeEstimate = document.getElementById('timeEstimate');
    const warningElement = document.getElementById('gameLimitWarning');
    
    function updateTimeEstimate() {
      const numGames = parseInt(gamesInput.value) || 0;
      const baseTime = 6; // Base time in seconds
      const perGameTime = 0.2; // Time per game in seconds
      
      // Calculate estimated time
      const estimatedTime = baseTime + (numGames * perGameTime);
      
      // Update the display
      timeEstimate.textContent = `Estimated processing time: ~${Math.round(estimatedTime)} seconds`;
      
      // Show/hide warning
      if (numGames > 1000) {
        warningElement.style.display = 'block';
        timeEstimate.classList.add('text-danger');
      } else {
        warningElement.style.display = 'none';
        timeEstimate.classList.remove('text-danger');
      }
    }
    
    // Update on input change and page load
    gamesInput.addEventListener('input', updateTimeEstimate);
    updateTimeEstimate(); // Initialize on page load
  });