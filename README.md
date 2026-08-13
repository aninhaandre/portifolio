# Thayná André — Penteados & Escovas

Tabela de preços digital para salão, feita para ser compartilhada no WhatsApp e
atualizada sem depender de designer.

**Site no ar:** [thaynaandrepenteados.netlify.app](https://thaynaandrepenteados.netlify.app)

![Prévia da tabela](assets/preview.png)

## O que o projeto tem

- **Identidade própria** — monograma TA em Bodoni Moda, paleta ameixa/pêssego/dourado
  e uma onda em degradê como assinatura da marca
- **Página em um arquivo só** (`index.html`) — HTML, CSS e JS juntos, com as fontes
  embutidas em WOFF2/base64: funciona offline e não depende do Google Fonts
- **Preços centralizados** — todos os valores ficam num bloco `TABELA` no fim do
  arquivo; economia e valor por penteado são calculados automaticamente
- **Pronta para o WhatsApp** — tags Open Graph com imagem de capa (`capa-whatsapp.png`),
  botão de agendamento com mensagem pré-preenchida via `wa.me`
- **PDF espelho** (`tabela-de-precos.pdf`) — versão A4 de uma página para envio direto
- **Detalhes** — textura de grão de papel, revelação ao rolar, cartão de destaque com
  brilho animado, `prefers-reduced-motion` respeitado, versão de impressão embutida

## Como atualizar os preços

1. Abra o `index.html` em qualquer editor de texto
2. Desça até o bloco `TABELA` (fim do arquivo)
3. Troque os números — nomes, valores, WhatsApp e Instagram estão todos ali
4. Republique no Netlify (arrastar e soltar) — a página recalcula tudo sozinha

## Stack

HTML + CSS + JavaScript puro, sem framework e sem build. Fontes: Bodoni Moda e
Jost (OFL), instanciadas e subsetadas com fontTools. PDF gerado com WeasyPrint.
