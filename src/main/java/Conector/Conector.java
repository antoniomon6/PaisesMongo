/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Conector;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import javax.swing.JOptionPane;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 *
 * @author casaMesaRider
 */
public class Conector {
    
    private final String bd= "periodico";
    private final String url ="mongodb://localhost:27017";
    private final String urlatlas="mongodb+srv://tiomicasa:tiomicasa@cluster.tn9zcjp.mongodb.net/?appName=Cluster";

    MongoDatabase Mongodb;
    
    public Conector() {
    }
    
    
    public boolean conectar(){
        boolean b=false;
        
        MongoClient mongo = MongoClients.create(urlatlas);
        
        if(mongo!=null){
            Mongodb = mongo.getDatabase(bd);
            b=true;
        }else{
            JOptionPane.showMessageDialog(null, "Error de conexión con MongoDB", "Error", JOptionPane.ERROR_MESSAGE);
        }
        
        return b;
    }
    
    public boolean altaUsuario (String dniv, String nombrev, String emailv){
        
        boolean b=false;
        
        if(dniv!=null){
            MongoCollection<Document> coleccion = Mongodb.getCollection("usuarios");
            
            try{
                Document usuario = new Document("dni",dniv)
                        .append("nombre",nombrev)
                        .append("email", emailv);
                coleccion.insertOne(usuario);
                b=true;
            }catch(MongoWriteException e){
                if(e.getCode()==11000){
                    System.out.println("Error: duplicidad en la clave");
                }else{
                    System.out.println("Erro de escritura en Mongo: "+e.getMessage());
                }
            }
        }else System.out.println("DNI sin valor");
        
        return b;
    }
    
    //Esta no es la forma que pide en la tarea
    public boolean altaNoticias (String dniv, String titulov, String cuerpov, String datev){
        
        boolean b=false;
        
        if(titulov!=null){
            MongoCollection<Document> coleccion = Mongodb.getCollection("noticias");
            
            try{
                Document noticia = new Document("Titulo",titulov)
                        .append("cuerpo",cuerpov)
                        .append("Fecha", datev)
                        .append("dni",dniv);
                coleccion.insertOne(noticia);
                b=true;
            }catch(MongoWriteException e){
                if(e.getCode()==11000){
                    System.out.println("Error: duplicidad en la clave");
                }else{
                    System.out.println("Error de escritura en Mongo: "+e.getMessage());
                }
            }
        }else System.out.println("Titulo sin valor");
        
        return b;
    }
    //Esta es la forma de la tarea
    public boolean altaNoticias1 (String dniv, String titulov, String cuerpov, String datev){
        
        boolean b=false;
        
        if(titulov!=null){
            MongoCollection<Document> coleccion = Mongodb.getCollection("noticias");
            MongoCollection<Document> usuarios = Mongodb.getCollection("usuarios");
            Document usuario = usuarios.find(new Document("dni",dniv)).first();
            
            ObjectId usuarioid=usuario.getObjectId("_id");
            
            try{
                Document noticia = new Document("Titulo",titulov)
                        .append("cuerpo",cuerpov)
                        .append("Fecha", datev)
                        .append("usuarioId",usuarioid);
                coleccion.insertOne(noticia);
                b=true;
            }catch(MongoWriteException e){
                if(e.getCode()==11000){
                    System.out.println("Error: duplicidad en la clave");
                }else{
                    System.out.println("Error de escritura en Mongo: "+e.getMessage());
                }
            }
        }else System.out.println("Titulo sin valor");
        
        return b;
    }
    
    public boolean ActualizarUsuario(String dniv, String nombrev, String emailv){
        boolean b=false;
        
        MongoCollection<Document> coleccion = Mongodb.getCollection("usuarios");
        Document doc = new Document("dni",dniv);
        
        if(doc!= null){
            Document act = new Document("$set", new Document("dni",dniv)
                    .append("nombre", nombrev)
                    .append("email", emailv));
            coleccion.updateOne(doc, act);
            b=true;
        }
        
        return b;
    }
    
    public boolean EliminarUsuario(String dniv){
        boolean b=false;
        
        MongoCollection<Document> coleccion = Mongodb.getCollection("usuarios");
        Document doc = new Document("dni",dniv);
        
        if(doc!= null){
            coleccion.findOneAndDelete(doc);
            b=true;
        }
        
        return b;
    }
    
    public ArrayList<Document> listar(){
        
        ArrayList<Document> lista = new ArrayList<>();
        
        MongoCollection<Document> coleccion = Mongodb.getCollection("usuarios");
        
        FindIterable<Document> documentos = coleccion.find();
        
        for (Document doc : documentos) {
            lista.add(doc);
        }
        
        return lista;
    }
    
    public String formatoFecha(Date fecha){
        String fechaCadena=null;
        SimpleDateFormat formato= new SimpleDateFormat("dd-MM-yyyy");
        fechaCadena= formato.format(fecha);
        return fechaCadena;
    }
    
    
            
}

