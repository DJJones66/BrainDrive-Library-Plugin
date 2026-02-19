const path = require("path");
const HtmlWebpackPlugin = require("html-webpack-plugin");
const { ModuleFederationPlugin } = require("webpack").container;
const packageJson = require("./package.json");
const deps = packageJson.dependencies;

const PLUGIN_SCOPE = "BrainDriveLibraryPlugin";
const DEV_PORT = 3008;
const VERSION = packageJson.version;

const EXPOSED_MODULES = {
  "./LibraryCapture": "./src/components/LibraryCapture",
  "./LibraryEditor": "./src/components/LibraryEditor",
};

const RELEASE_PATH = "dist";
const LOCAL_PATH = `../../backend/plugins/shared/${PLUGIN_SCOPE}/v${VERSION}/dist`;

module.exports = (env = {}) => {
  const isRelease = env.release === true || env.release === "true";

  return {
    mode: isRelease ? "production" : "development",

    entry: "./src/index",

    output: {
      path: path.resolve(__dirname, isRelease ? RELEASE_PATH : LOCAL_PATH),
      publicPath: "auto",
      clean: true,
      library: {
        type: "var",
        name: PLUGIN_SCOPE
      }
    },

    resolve: {
      extensions: [".tsx", ".ts", ".js"]
    },

    module: {
      rules: [
        {
          test: /\.(ts|tsx)$/,
          use: "ts-loader",
          exclude: [/node_modules/, /__tests__/]
        },
        {
          test: /\.css$/,
          use: ["style-loader", "css-loader"]
        }
      ]
    },

    plugins: [
      new ModuleFederationPlugin({
        name: PLUGIN_SCOPE,
        library: { type: "var", name: PLUGIN_SCOPE },
        filename: "remoteEntry.js",
        exposes: EXPOSED_MODULES,
        shared: {
          react: {
            singleton: true,
            eager: true,
            requiredVersion: deps.react
          },
          "react-dom": {
            singleton: true,
            eager: true,
            requiredVersion: deps["react-dom"]
          }
        }
      }),

      new HtmlWebpackPlugin({
        template: "./public/index.html"
      })
    ],

    devServer: {
      port: DEV_PORT,
      static: {
        directory: path.join(__dirname, "public")
      },
      hot: true
    }
  };
};
