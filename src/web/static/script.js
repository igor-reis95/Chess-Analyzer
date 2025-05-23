document.addEventListener("DOMContentLoaded", function() {
  const toggleButton = document.getElementById("toggleTable");
  const gamesTable = document.getElementById("gamesTable");

  toggleButton.addEventListener("click", function() {
    if (gamesTable.style.display === "none") {
      gamesTable.style.display = "table";
      toggleButton.textContent = "Hide Full Games Table ▲";
    } else {
      gamesTable.style.display = "none";
      toggleButton.textContent = "Show Full Games Table ▼";
    }
  });
});