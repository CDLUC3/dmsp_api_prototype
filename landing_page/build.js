const esbuild = require('esbuild');
const fs = require('fs');
const path = require('path');
// Import the Sass plugin
const { sassPlugin } = require('esbuild-sass-plugin');

// Ensure the build directory exists
if (!fs.existsSync('build')) {
    fs.mkdirSync('build');
}

// Copy your index.html template over to the build folder
fs.copyFileSync(
    path.join(__dirname, 'public/index.html'),
    path.join(__dirname, 'build/index.html')
);

// Run the esbuild bundle
esbuild.build({
    entryPoints: ['src/index.js'], // Swap to .tsx or .jsx if applicable
    bundle: true,
    minify: true,
    sourcemap: true,
    outfile: 'build/bundle.js',
    loader: { '.js': 'jsx', '.png': 'dataurl', '.svg': 'text' },
    // Register the plugin here
    plugins: [
        sassPlugin()
    ],
})
    .then(() => console.log('⚡ Build complete! Ready for CloudFront.'))
    .catch((e) => {
      console.log('Build failed', e)
      process.exit(1)
    });