// Ajusta ano do rodapé
document.getElementById("year").textContent = new Date().getFullYear();

// Simulação de envio de formulário (JavaScript)
const form = document.getElementById("contact-form");
if (form) {
  form.addEventListener("submit", (event) => {
    event.preventDefault();

    const formData = new FormData(form);
    const nome = formData.get("nome");

    alert(
      `Obrigada pela mensagem, ${nome || "pessoa"}!\n\n` +
        "Este formulário é apenas uma simulação no front-end.\n" +
        "Para torná-lo funcional, conecte-o a um backend ou serviço de envio de e-mail."
    );

    form.reset();
  });
}

