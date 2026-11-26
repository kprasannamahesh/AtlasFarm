module.exports = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#172033",
        muted: "#68748a",
        canvas: "#f5f7fb",
        brand: "#4361ee",
      },
      boxShadow: {
        card: "0 12px 32px rgba(27, 39, 67, 0.06)",
      },
    },
  },
  plugins: [],
};
