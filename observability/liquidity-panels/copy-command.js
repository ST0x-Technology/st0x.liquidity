// The Copy button beside each recovery command in the header's guide and the
// row dialog. The generator prepends this file to header.js and detail.js;
// dashboard/src/lib/board-copy-command.test.ts imports it as a module through
// the export line, which the generator drops.

// The markup of one command: its text in a <pre> and the button that copies
// it. `commandHtml` is already escaped.
const commandLine = (commandHtml, preClass) =>
  `<div class="liq-command-line"><pre${preClass ? ` class="${preClass}"` : ''}>${commandHtml}</pre>` +
  '<button type="button" class="liq-copy" data-copy>Copy</button></div>';

// Wires every Copy button under `container` to its command. Property
// handlers, so a render that keeps the buttons replaces them instead of
// stacking another copy. The button says how the copy went for a moment.
const wireCopyButtons = (container) => {
  container.querySelectorAll('[data-copy]').forEach((button) => {
    button.onclick = () => {
      const command = button.parentElement.querySelector('pre').textContent;
      const show = (label) => {
        button.textContent = label;
        clearTimeout(button.liqCopyReset);
        button.liqCopyReset = setTimeout(() => {
          button.textContent = 'Copy';
        }, 1500);
      };
      if (!navigator.clipboard) {
        show('Copy failed');
        return;
      }
      navigator.clipboard.writeText(command).then(
        () => show('Copied'),
        () => show('Copy failed')
      );
    };
  });
};

export { commandLine, wireCopyButtons };
