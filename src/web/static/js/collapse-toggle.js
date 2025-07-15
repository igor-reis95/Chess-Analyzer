// Toggle button text for collapse elements
document.querySelectorAll('[data-bs-toggle="collapse"]').forEach(button => {
    button.addEventListener('click', function() {
        const spans = this.querySelectorAll('span');
        spans.forEach(span => span.classList.toggle('d-none'));
    });
});