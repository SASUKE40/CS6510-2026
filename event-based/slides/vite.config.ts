const gateway = process.env.CHECKOUT_GATEWAY ?? 'http://localhost:8080'

// @unocss/preset-icons skips its local icon loader when VSCODE_CWD is set, which
// VS Code and Cursor terminals do. Slidev's toolbar icons then render empty.
delete process.env.VSCODE_CWD

export default {
  server: {
    proxy: {
      '/api': {
        target: gateway,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/api/, ''),
      },
    },
  },
}
