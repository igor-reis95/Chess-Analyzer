document.addEventListener('DOMContentLoaded', function() {
  function updateTimeControls() {
    const platform = document.querySelector('[name="platform"]:checked').value;
    const classicalOption = document.querySelector('#perf_type option[value="classical"]');
    
    if (platform === 'chess.com') {
      if (classicalOption) classicalOption.remove();
    } else if (!classicalOption) {
      const select = document.getElementById('perf_type');
      select.innerHTML += '<option value="classical">Classical</option>';
    }
  }

  // Initialize
  updateTimeControls();
  
  // Update when platform changes
  document.querySelectorAll('[name="platform"]').forEach(radio => {
    radio.addEventListener('change', updateTimeControls);
  });
});